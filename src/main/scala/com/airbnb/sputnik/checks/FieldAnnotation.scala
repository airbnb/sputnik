package com.airbnb.sputnik.checks

import java.lang.annotation.Annotation


trait FieldAnnotation {

  def fromAnnotation(annotation: Annotation, fieldName: String): Check

  def getAnnotationType: Class[_]

}
