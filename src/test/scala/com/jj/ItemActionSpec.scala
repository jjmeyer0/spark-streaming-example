package com.jj

import com.holdenkarau.spark.testing.SharedSparkContext
import com.jj.service.ItemAction
import com.jj.utils.ItemTestUtils
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._

class ItemActionSpec extends FunSuite with SharedSparkContext with MockitoSugar  {
  // issue in spark test base. See: https://github.com/holdenk/spark-testing-base/issues/33
  override def mock[T <: AnyRef](implicit manifest: Manifest[T]): T = super.mock[T](withSettings().serializable())

  test("ItemAction perform on RDD should properly send data to the correct topic") {
    val kafkaSender = mock[KafkaSender]
    val broadcast = sc.broadcast(kafkaSender)

    val items = sc.parallelize(Seq(ItemTestUtils.basicEntity))
    val topic = "processed_item"
    ItemAction.performRdd(items, Set(topic), broadcast)

    verify(kafkaSender).send(same(topic), anyString())
  }
}

