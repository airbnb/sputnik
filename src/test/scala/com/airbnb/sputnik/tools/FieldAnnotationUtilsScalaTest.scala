package com.airbnb.sputnik.tools

import com.airbnb.sputnik.annotations.Comment
import com.airbnb.sputnik.tools.FieldAnnotationUtilsScalaTest._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

object FieldAnnotationUtilsScalaTest {

  case class NoAnnotations(a: String,
                           b: Int
                          )

  case class SomeAnnotations(
                              @Comment("Some comment") a: String,
                              @Comment("some other comment") b: Int
                            )

}

@RunWith(classOf[JUnitRunner])
class FieldAnnotationUtilsScalaTest extends FunSuite {

  test("test handling case class") {
    assert(FieldAnnotationUtils.getFieldsAnnotations(classOf[NoAnnotations]).isEmpty)
    val annotations = FieldAnnotationUtils.getFieldsAnnotations(classOf[SomeAnnotations])
    assert(annotations.size == 2)
    assert(annotations.get("a").get.head.asInstanceOf[Comment].value() == "Some comment")
    assert(annotations.get("b").get.head.asInstanceOf[Comment].value() == "some other comment")
  }

}
