package com.lucho.models

import spray.json.{JsString, JsValue, JsonFormat}

object Country extends Enumeration {

  type Country = Value

  val argentina = Value

  implicit object CountryFormat extends JsonFormat[Country] {
    override def read(json: JsValue) = json match {
      case JsString("argentina") => Country.argentina
      case _ => throw new Exception("Invalid JsValue type for Country conversion: must be JsString")
    }

    override def write(c: Country) = c match {
      case Country.argentina => JsString("argentina")
    }
  }

}
