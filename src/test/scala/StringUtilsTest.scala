import org.scalatest.funsuite.AnyFunSuite

class StringUtilsTest extends AnyFunSuite {

    test("Test removePlus91") {
        val testString = "+916000000001"
        assertResult("6000000001") {
            StringUtils.removePlus91(testString)
        }
    }

}
