package com.jj.streaming.service

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.jj.model.Item
import com.jj.utils.ItemTestUtils
import org.scalatest.mock.MockitoSugar

class ItemProcessorSpec extends StreamingSuiteBase with MockitoSugar {
  test("ItemProcessor should properly process all ") {
    val data: Seq[Seq[(String,String)]] = Seq(
      Seq(
        ("1", ItemTestUtils.basicEntityString),
        ("2", ItemTestUtils.basicEntityString2),
        ("3", ItemTestUtils.basicEntityString3)
      )
    )

    val expected: Seq[Seq[Item]] = Seq(
      Seq(
        ItemTestUtils.basicEntity,
        ItemTestUtils.basicEntity2,
        ItemTestUtils.basicEntity3
      )
    )

    testOperation(data, ItemProcessor.process _, expected, ordered = false)
  }
}
