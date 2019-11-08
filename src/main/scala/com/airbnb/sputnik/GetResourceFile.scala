package com.airbnb.sputnik

import org.apache.commons.io.IOUtils

trait GetResourceFile {

  def getFileContent(path: String): String = {
    val inputStream = getClass.getClassLoader.getResourceAsStream(path)
    try {
      IOUtils.toString(inputStream)
    } finally {
      inputStream.close()
    }
  }

}
