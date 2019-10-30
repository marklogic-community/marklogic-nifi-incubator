import java.security.MessageDigest


FlowFile flowFile = session.get()

if (!flowFile) {
    return
}

try {
    String email = flowFile.getAttribute('email')

    MessageDigest digest = MessageDigest.getInstance("SHA-256")
    byte[] hash = digest.digest(email.getBytes("UTF-8"))
    StringBuffer hexString = new StringBuffer()

    for (int i = 0; i < hash.length; i++) {
        String hex = Integer.toHexString(0xff & hash[i])
        if (hex.length() == 1) hexString.append('0')
        hexString.append(hex)
    }

    String emailHash = hexString.toString()

    flowFile = session.putAttribute(flowFile, 'emailHash', emailHash)
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error("There was an error hashing the email attribute")

    session.transfer(flowFile, REL_FAILURE)
}