package br.com.rendmais.p2p.net;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class NetUtils {

    public static String localAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }
}
