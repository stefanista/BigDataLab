package graphframes-3e6cfe8fec757e76fa041b8f96f2731cae77a353.src.main.scala.org.graphframes

// All the public exceptions thrown by GraphFrame methods


/**
 * Exception thrown when a pattern String for motif finding cannot be parsed.
 */
class InvalidParseException(message: String) extends Exception(message)

/**
 * Thrown when a GraphFrame algorithm is given a vertex ID which does not exist in the graph.
 */
class NoSuchVertexException(message: String) extends Exception(message)

/**
 * Exception thrown when a parsed pattern for motif finding cannot be translated into a DataFrame
 * query.
 */
class InvalidPatternException() extends Exception()