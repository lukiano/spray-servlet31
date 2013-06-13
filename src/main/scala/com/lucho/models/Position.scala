package com.lucho.models

import spray.json.DefaultJsonProtocol


case class Position(latitude: Double, longitude: Double)

object PositionProtocol extends DefaultJsonProtocol {

  implicit val positionFormat = jsonFormat2(Position)

}



