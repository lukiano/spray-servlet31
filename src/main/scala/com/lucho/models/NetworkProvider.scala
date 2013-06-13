package com.lucho.models

import spray.json.{JsString, JsValue, JsonFormat, DefaultJsonProtocol}

sealed abstract case class NetworkProvider(country: Country.Country) {
  val name: String
}

object personal extends NetworkProvider(Country.argentina) {
  val name = "personal"
}

object movistar extends NetworkProvider(Country.argentina) {
  val name = "movistar"
}

object NetworkProvider {

  def apply(provider: String): NetworkProvider = provider match {
      case "personal" => personal
      case "movistar" => movistar
  }


}

object NetworkProviderProtocol extends DefaultJsonProtocol {

  implicit object NetworkProviderFormat extends JsonFormat[NetworkProvider] {
    override def read(json: JsValue) = json match {
      case JsString(provider)                 => NetworkProvider(provider)
      case _                                  => throw new Exception("Invalid JsValue type for NetworkProvider conversion: JsString")
    }

    override def write(np: NetworkProvider) = JsString(np.name)
  }

}

