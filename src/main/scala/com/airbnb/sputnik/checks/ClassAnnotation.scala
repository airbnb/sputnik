package com.airbnb.sputnik.checks

import java.lang.annotation.Annotation


trait ClassAnnotation {

  def fromAnnotation(annotation: Annotation): Check

  def getAnnotationType: Class[_ <: java.lang.annotation.Annotation]
}
