package com.jj.utils

import com.jj.model.Item

object ItemTestUtils {

  val basicEntityString = """{"id":1,"name":"name","description":"description"}"""
  val basicEntityString2 = """{"id":2,"name":"name2","description":"description2"}"""
  val basicEntityString3 = """{"id":3,"name":"name3","description":"description3"}"""

  val basicEntity = Item(1, "name", "description")
  val basicEntity2 = Item(2, "name2", "description2")
  val basicEntity3 = Item(3, "name3", "description3")

}
