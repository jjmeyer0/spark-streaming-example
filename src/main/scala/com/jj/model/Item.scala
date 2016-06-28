package com.jj.model

import play.api.libs.json._

case class Item(id: Long, name: String, description: String)

object Item {
  implicit val formatter = Json.format[Item]
}
