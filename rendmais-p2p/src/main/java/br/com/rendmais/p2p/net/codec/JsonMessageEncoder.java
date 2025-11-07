package br.com.rendmais.p2p.net.codec;

import br.com.rendmais.common.dto.SignedMessage;
import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class JsonMessageEncoder extends MessageToByteEncoder<SignedMessage> {

    private final Gson gson = new Gson();

    @Override
    protected void encode(ChannelHandlerContext ctx, SignedMessage msg, ByteBuf out) throws Exception {
        String json = gson.toJson(msg);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        out.writeBytes(bytes);
    }
}