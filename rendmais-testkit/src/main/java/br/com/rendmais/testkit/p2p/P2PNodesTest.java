package br.com.rendmais.testkit.p2p;

import br.com.rendmais.p2p.P2PNodeBootstrap;

public class P2PNodesTest {
    public static void main(String[] args) throws Exception {
        P2PNodeBootstrap nodeA = new P2PNodeBootstrap(7000);
        P2PNodeBootstrap nodeB = new P2PNodeBootstrap(7001);

        nodeA.start();
        nodeB.start();

        // nodeA conecta ao nodeB
        nodeA.connectTo("127.0.0.1", 7001);

        // nodeB conecta ao nodeA (optional)
        nodeB.connectTo("127.0.0.1", 7000);

        Thread.sleep(5000);

        System.out.println("NodeA known peers: " + nodeA.getRegistry().listPeers().size());
        System.out.println("NodeB known peers: " + nodeB.getRegistry().listPeers().size());

        nodeA.stop();
        nodeB.stop();
    }
}
