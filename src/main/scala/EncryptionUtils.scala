import java.security.MessageDigest

object EncryptionUtils {

    val sha256Hash = (inputString: String) => {
        val messageDigest = MessageDigest.getInstance("SHA-256")
        val bytes = messageDigest.digest(inputString.getBytes("UTF-8"))
        val hexString = new StringBuilder

        for (byte <- bytes) {
            val hex = Integer.toHexString(0xff & byte)
            if (hex.length == 1) hexString.append('0')
            hexString.append(hex)
        }

        hexString.toString
    }

}
