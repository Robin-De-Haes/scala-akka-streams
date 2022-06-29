// Helper class to store the total number of libraries that have a minimum number of dependencies
case class LibraryCount(lowerLimit : Int, compileCount: Int, providedCount: Int,
                        runtimeCount: Int, testCount: Int)
{
  override def toString: String =
    s"Considered minimum number of dependencies: ${lowerLimit}\n" +
      s"Compile: ${compileCount}\n" +
      s"Provided: ${providedCount}\n" +
      s"Runtime: ${runtimeCount}\n" +
      s"Test: ${testCount}"
}