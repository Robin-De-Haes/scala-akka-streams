// Helper class for representing a library dependency record
// The library attribute could perhaps also be represented by an object with 3 components
// (GroupID, ArtifactID, Version), but this is not needed for our purposes so it is left as a String
case class LibraryDependency(library: String,
                             dependency: String,
                             dependencyType: String)
{
  def isCompileDependency: Boolean = {
    "Compile".equalsIgnoreCase(dependencyType)
  }

  def isProvidedDependency: Boolean = {
    "Provided".equalsIgnoreCase(dependencyType)
  }

  def isRuntimeDependency: Boolean = {
    "Runtime".equalsIgnoreCase(dependencyType)
  }

  def isTestDependency: Boolean = {
    "Test".equalsIgnoreCase(dependencyType)
  }
}