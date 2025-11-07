package br.com.rendmais.common.crypto;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public final class KeyUtil {

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private KeyUtil() {}

    public static KeyPair generateEd25519KeyPair() {
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("Ed25519", "BC");
            return kpg.generateKeyPair();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate Ed25519 keypair", e);
        }
    }

    public static String publicKeyToBase64(PublicKey pk) {
        return Base64.getEncoder().encodeToString(pk.getEncoded());
    }

    public static String privateKeyToBase64(PrivateKey pk) {
        return Base64.getEncoder().encodeToString(pk.getEncoded());
    }

    public static PublicKey publicKeyFromBase64(String base64) {
        try {
            byte[] data = Base64.getDecoder().decode(base64);
            X509EncodedKeySpec spec = new X509EncodedKeySpec(data);
            KeyFactory kf = KeyFactory.getInstance("Ed25519", "BC");
            return kf.generatePublic(spec);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load public key", e);
        }
    }

    public static byte[] sign(PrivateKey privateKey, byte[] data) {
        try {
            Signature sig = Signature.getInstance("Ed25519", "BC");
            sig.initSign(privateKey);
            sig.update(data);
            return sig.sign();
        } catch (Exception e) {
            throw new RuntimeException("Failed to sign", e);
        }
    }

    public static boolean verify(PublicKey publicKey, byte[] data, byte[] signature) {
        try {
            Signature sig = Signature.getInstance("Ed25519", "BC");
            sig.initVerify(publicKey);
            sig.update(data);
            return sig.verify(signature);
        } catch (Exception e) {
            throw new RuntimeException("Failed to verify signature", e);
        }
    }

    // Simple file persistence (private key in PKCS8 DER format)
    public static void saveKeyPairToFile(KeyPair kp, Path path) {
        try {
            Files.createDirectories(path.getParent());
            try (DataOutputStream out = new DataOutputStream(new FileOutputStream(path.toFile()))) {
                byte[] priv = kp.getPrivate().getEncoded();
                byte[] pub = kp.getPublic().getEncoded();
                out.writeInt(priv.length);
                out.write(priv);
                out.writeInt(pub.length);
                out.write(pub);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save keypair", e);
        }
    }

    public static KeyPair loadKeyPairFromFile(Path path) {
        try (DataInputStream in = new DataInputStream(new FileInputStream(path.toFile()))) {
            int privLen = in.readInt();
            byte[] priv = new byte[privLen];
            in.readFully(priv);
            int pubLen = in.readInt();
            byte[] pub = new byte[pubLen];
            in.readFully(pub);

            KeyFactory kf = KeyFactory.getInstance("Ed25519", "BC");
            // PrivateKey from PKCS8
            PrivateKey privateKey = kf.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(priv));
            PublicKey publicKey = kf.generatePublic(new X509EncodedKeySpec(pub));
            return new KeyPair(publicKey, privateKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load keypair", e);
        }
    }
}
