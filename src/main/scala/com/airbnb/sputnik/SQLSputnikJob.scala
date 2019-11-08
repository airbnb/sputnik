package com.airbnb.sputnik

trait SQLSputnikJob extends SputnikJob with Logging with GetResourceFile {

  def executeResourceSQLFile(path: String) = {
    val query = getFileContent(path)
    executeSQLQuery(query)
  }

  def executeSQLQuery(query: String) = {
    logger.info(s"Executing query: $query")
    sputnikSession.ss.sql(query)
  }

}
