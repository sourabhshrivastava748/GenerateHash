import org.scalatest.funsuite.AnyFunSuite

class EncryptionUtilsTest extends AnyFunSuite {

    test("Convert to SHA256 test 1") {
        val testString = "+916000000001"
        assertResult("ffdbe12d10dca9a543d5c7495d0a2b1ab8c083099f2570b5f29e5c2c7fc0b6fc") {
            EncryptionUtils.sha256Hash(testString)
        }
    }

    test("Convert to SHA256 test 2") {
        val testString = "+916000000003"
        assertResult("cd4d4edb313f8c3acfc802851649e380e9b2c83443d8f51503496b5a75aecaa2") {
            EncryptionUtils.sha256Hash(testString)
        }
    }

}
