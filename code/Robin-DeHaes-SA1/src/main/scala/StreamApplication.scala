import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Balance, Broadcast, FileIO, Flow, GraphDSL, Keep, Merge, RunnableGraph, Sink, Source, ZipWith4, ZipWith5}
import akka.util.ByteString

import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING, WRITE}
import java.nio.file.{Path, Paths}
import scala.concurrent.{ExecutionContextExecutor, Future}

object StreamApplication extends App {
  // Lower limit for the predefined number of dependencies (Statistics Extension)
  val lowerLimit : Int = 2

  // Initialize implicits needed for stream materialization
  implicit val actorSystem: ActorSystem = ActorSystem("StreamApplication")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

  // Get file with maven dependencies
  val resourcesFolder: String = "src/main/resources"
  val pathTxtFile: Path = Paths.get(s"$resourcesFolder/maven_dependencies.txt")
  val file: scala.io.Source = scala.io.Source.fromFile(s"$resourcesFolder/maven_dependencies.txt")

  // Create a Source composed of the lines from the file
  val txtSource: Source[String, NotUsed] =
    Source.fromIterator(() => file.getLines)

  // Parse the txt lines (with comma-separated attributes)
  val lineParsing: Flow[String, Array[String], NotUsed] =
    Flow[String].map(_.split(","))

  // Filter out invalid entries (exactly 3 non-empty values are needed per line),
  // although all entries can probably be presumed to be valid.
  // Invalid entries could be written to another file so reviewing them would be possible
  // or some other error handling could be added, but this is out-of-scope for the assignment.
  val invalidDependencyFilter: Flow[Array[String], Array[String], NotUsed] =
    Flow[Array[String]].filter(_.count(_.nonEmpty) == 3)

  // Convert row contents to LibraryDependency objects
  val conversionToLibraryDependency: Flow[Array[String], LibraryDependency, NotUsed] =
    Flow[Array[String]].map(contentsArray => {
      LibraryDependency(contentsArray(0), contentsArray(1), contentsArray(2))
    })

  // Create a source generating LibraryDependency objects based on the given maven_dependencies file
  val libraryDependencySource: Source[LibraryDependency, NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // Add required graph elements to the builder
      val lineGenerator = builder.add(txtSource)
      val lineParser = builder.add(lineParsing)
      val dependencyFilter = builder.add(invalidDependencyFilter)
      val converter = builder.add(conversionToLibraryDependency)

      // Connect the graph components
      lineGenerator ~> lineParser ~> dependencyFilter ~> converter

      // Expose port
      SourceShape(converter.out)
    })

  // Group LibraryDependency objects together by library name,
  // so we end up with a stream of lists of dependencies for the same library after merging.
  // This allows easy parallel processing later on.
  val groupedByLibrary : Source[List[LibraryDependency], NotUsed] =
    libraryDependencySource.groupBy(Int.MaxValue, _.library)
      .fold[List[LibraryDependency]](List())((currentList, libraryDependency) => {
        currentList :+ libraryDependency
      }).mergeSubstreams

  // Generic filter for counting the number of elements in a stream
  val elementCounter: Flow[Any, Int, NotUsed] = Flow[Any].fold[Int](0)((count, _) => count + 1)

  // Filter for each dependency type
  val compileFilter: Flow[LibraryDependency, LibraryDependency, NotUsed] = Flow[LibraryDependency].filter(_.isCompileDependency)
  val providedFilter: Flow[LibraryDependency, LibraryDependency, NotUsed] = Flow[LibraryDependency].filter(_.isProvidedDependency)
  val runtimeFilter: Flow[LibraryDependency, LibraryDependency, NotUsed] = Flow[LibraryDependency].filter(_.isRuntimeDependency)
  val testFilter: Flow[LibraryDependency, LibraryDependency, NotUsed] = Flow[LibraryDependency].filter(_.isTestDependency)

  // Counts the number of dependencies of each type in the stream
  // and aggregates them into one AggregatedDependencyCount object
  val dependencyCounter: Graph[FlowShape[LibraryDependency, AggregatedDependencyCount], NotUsed] = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // Filter to get the library name of the current LibraryDependency
      // It is enough to only take one element, since the library name is the same for all dependencies
      // that are counted for the creation of one zipped object and each subpart that filters and counts 1 type of
      // dependency will deliver 1 resulting element as well (with the total count for that dependency type).
      val libraryFilter: Flow[LibraryDependency, String, NotUsed] = Flow[LibraryDependency].take(1).map(_.library)
      val libraryFlow = builder.add(libraryFilter)

      // Broadcast and zip are used to be able to process the different dependency types in parallel
      // We use 5 parallel streams: one for each dependency type (so 4 in total) and 1 for the library name
      val broadcast = builder.add(Broadcast[LibraryDependency](5))
      val zipValues = builder.add(new ZipWith5[String, Int, Int, Int, Int, AggregatedDependencyCount](
        (libraryName, compileDependencyCount, providedDependencyCount, runtimeDependencyCount, testDependencyCount) => {
          AggregatedDependencyCount(libraryName, compileDependencyCount, providedDependencyCount, runtimeDependencyCount, testDependencyCount)
        }
      ))

      // Connect the graph components
      broadcast ~> libraryFlow ~> zipValues.in0
      broadcast ~> compileFilter ~> elementCounter ~> zipValues.in1
      broadcast ~> providedFilter ~> elementCounter ~> zipValues.in2
      broadcast ~> runtimeFilter ~> elementCounter ~> zipValues.in3
      broadcast ~> testFilter ~> elementCounter ~> zipValues.in4

      // Expose ports
      FlowShape(broadcast.in, zipValues.out)
    }
  )

  // Transform stream of List[LibraryDependency] to a stream of related LibraryDependency objects
  // that are processed by the dependencyCounter (i.e. count the dependencies for 1 LibraryDependency List)
  // and deliver an AggregatedDependencyCount for each processed List
  val flatMapFlow: Flow[List[LibraryDependency], AggregatedDependencyCount, NotUsed] =
    Flow[List[LibraryDependency]].flatMapConcat(Source(_).via(dependencyCounter))

  // Process the Library Dependencies in 2 parallel pipelines
  val parallelDependencyCounters: Graph[FlowShape[List[LibraryDependency], AggregatedDependencyCount], NotUsed] = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // Balance and merge are used to be able to process the groups of libraries in parallel
      // We use 2 parallel pipelines in a balanced approach
      val balance = builder.add(Balance[List[LibraryDependency]](2))
      val merge = builder.add(Merge[AggregatedDependencyCount](2))

      // Connect the graph components
      balance ~> flatMapFlow ~> merge.in(0)
      balance ~> flatMapFlow ~> merge.in(1)

      // Expose ports
      FlowShape(balance.in, merge.out)
    }
  )

  // Create or overwrite existing dependency count file
  val dependenciesSink : Sink[AggregatedDependencyCount, Future[IOResult]] =
    Flow[AggregatedDependencyCount]
      .map(count => ByteString(s"$count\n"))
      .toMat(FileIO.toPath(Paths.get(s"$resourcesFolder/results/library_dependency_count.txt"),
                                     Set(CREATE, WRITE, TRUNCATE_EXISTING)))(Keep.right)

  // EXTENSION
  // Provides a statistics Flow counting the number of libraries with at least the specified lower limit
  // of number of dependencies (per dependency type)
  def libraryCounter(lowerLimit : Int = lowerLimit): Graph[FlowShape[AggregatedDependencyCount, LibraryCount], NotUsed]  = {
    // Counts the number of dependencies of each type in the stream
    // and aggregates them into one LibraryCount object
    Flow.fromGraph(
      GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._

        // Filter for each dependency type
        val compileCountFilter: Flow[AggregatedDependencyCount, AggregatedDependencyCount, NotUsed] =
          Flow[AggregatedDependencyCount].filter(_.compileDependencyCount >= lowerLimit)
        val providedCountFilter: Flow[AggregatedDependencyCount, AggregatedDependencyCount, NotUsed] =
          Flow[AggregatedDependencyCount].filter(_.providedDependencyCount >= lowerLimit)
        val runtimeCountFilter: Flow[AggregatedDependencyCount, AggregatedDependencyCount, NotUsed] =
          Flow[AggregatedDependencyCount].filter(_.runtimeDependencyCount >= lowerLimit)
        val testCountFilter: Flow[AggregatedDependencyCount, AggregatedDependencyCount, NotUsed] =
          Flow[AggregatedDependencyCount].filter(_.testDependencyCount >= lowerLimit)

        // Add elements to the builder
        val compileFlow = builder.add(compileCountFilter)
        val providedFlow = builder.add(providedCountFilter)
        val runtimeFlow = builder.add(runtimeCountFilter)
        val testFlow = builder.add(testCountFilter)

        // Broadcast and zip are used to be able to process the different dependency types in parallel
        // We use 4 parallel streams: one for each dependency type (so 4 in total)
        val broadcast = builder.add(Broadcast[AggregatedDependencyCount](4))
        val zipValues = builder.add(new ZipWith4[Int, Int, Int, Int, LibraryCount](
          (compileDependencyCount, providedDependencyCount, runtimeDependencyCount, testDependencyCount) => {
            LibraryCount(lowerLimit, compileDependencyCount, providedDependencyCount, runtimeDependencyCount, testDependencyCount)
          }
        ))

        // Connect the graph components
        broadcast ~> compileFlow ~> elementCounter ~> zipValues.in0
        broadcast ~> providedFlow ~> elementCounter ~> zipValues.in1
        broadcast ~> runtimeFlow ~> elementCounter ~> zipValues.in2
        broadcast ~> testFlow ~> elementCounter ~> zipValues.in3

        // Expose ports
        FlowShape(broadcast.in, zipValues.out)
      }
    )
  }

  // Create or overwrite existing statistics file
  val statisticsSink : Sink[LibraryCount, Future[IOResult]] =
    Flow[LibraryCount]
      .map(count => ByteString(s"$count\n"))
      .toMat(FileIO.toPath(Paths.get(s"$resourcesFolder/results/statistics.txt"),
        Set(CREATE, WRITE, TRUNCATE_EXISTING)))(Keep.right)

  // Run the graph to create the resulting library_dependency_count.txt and statistics.txt files
  val runnableGraph: RunnableGraph[Future[IOResult]] =
    groupedByLibrary.via(parallelDependencyCounters)
      .alsoToMat(dependenciesSink)(Keep.right) // to dependencies sink
      .via(libraryCounter(lowerLimit)) // considered minimum number of dependencies
      .toMat(statisticsSink)(Keep.right) // to statistics sink

  // Clean up the created actor systems
  runnableGraph.run().foreach(_ => actorSystem.terminate())
}
