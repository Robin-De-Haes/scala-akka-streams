// Helper class to store the total number of dependencies per type for the given library (name)
case class AggregatedDependencyCount(libraryName: String, compileDependencyCount: Int, providedDependencyCount: Int,
                                     runtimeDependencyCount: Int, testDependencyCount: Int)
{
  override def toString: String = {
    s"$libraryName --> " +
      s"Compile: ${compileDependencyCount} " +
      s"Provided: ${providedDependencyCount} " +
      s"Runtime: ${runtimeDependencyCount} " +
      s"Test: ${testDependencyCount}"
  }
}