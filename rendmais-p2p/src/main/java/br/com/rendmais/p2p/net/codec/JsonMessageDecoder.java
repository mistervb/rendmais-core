package br.com.rendmais.p2p.net.codec;

import br.com.rendmais.common.dto.SignedMessage;
import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class JsonMessageDecoder extends ByteToMessageDecoder {

    private final Gson gson = new Gson();

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int readable = in.readableBytes();
        if (readable == 0) return;
        byte[] bytes = new byte[readable];
        in.readBytes(bytes);
        String json = new String(bytes, StandardCharsets.UTF_8);
        SignedMessage msg = gson.fromJson(json, SignedMessage.class);
        out.add(msg);
    }
}