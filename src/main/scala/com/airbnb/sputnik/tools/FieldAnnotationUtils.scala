package com.airbnb.sputnik.tools

import java.lang.annotation.Annotation
import java.lang.reflect.Modifier

object FieldAnnotationUtils {

  type FieldName = String

  def getFieldsAnnotation(itemClass: Class[_], annotationClass: Class[_]): Map[FieldName, Annotation] = {
    val allAnnotations = getFieldsAnnotations(itemClass)
    allAnnotations.flatMap({ case (field, annotations) => {
      annotations.find(annotation => {
        annotation.annotationType().equals(annotationClass)
      })
        .map(annotation => field -> annotation)
    }
    })
  }

  def getFields(itemClass: Class[_]) = {
    itemClass
      .getDeclaredFields
      .filter(field => !field.getName.equals("$jacocoData"))
      .filter(field => !(Modifier.isStatic(field.getModifiers)))
  }

  def getFieldsAnnotations(itemClass: Class[_]): Map[FieldName, Array[Annotation]] = {

    val fields = getFields(itemClass)

    val annotationsMap = fields
      .flatMap(field => {
        if (field.getAnnotations.isEmpty) {
          None
        } else {
          Some(field.getName -> field.getAnnotations)
        }
      })
      .toMap

    if (annotationsMap.isEmpty) {
      // probably case class
      val constructors = itemClass.getConstructors

      val fieldIndexes = fields
        .zipWithIndex
        .map({ case (field, i) => i -> field.getName })
        .toMap

      constructors.head
        .getParameters
        .zipWithIndex
        .flatMap({ case (param, i) =>
          if (param.getAnnotations.isEmpty) {
            None
          } else {
            Some(fieldIndexes.get(i).get -> param.getAnnotations)
          }
        })
        .toMap

    } else {
      annotationsMap
    }


  }

}
