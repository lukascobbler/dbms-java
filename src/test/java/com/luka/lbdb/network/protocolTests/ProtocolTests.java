package com.luka.lbdb.network.protocolTests;

import com.luka.lbdb.querying.virtualEntities.constant.*;
import com.luka.lbdb.records.schema.Schema;
import com.luka.lbdb.network.protocol.Protocol;
import com.luka.lbdb.network.protocol.response.EmptySet;
import com.luka.lbdb.network.protocol.response.ErrorResponse;
import com.luka.lbdb.network.protocol.response.QuerySet;
import com.luka.lbdb.network.protocol.response.Response;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProtocolTests {
    @Test
    public void serializeEmptyResult() throws IOException {
        for (int i = 0; i < 1000000; i++) {
            Response emptyResponse = new EmptySet(i);

            byte[] bytes = Protocol.serialize(emptyResponse);
            byte[] bytesWithoutLength = Arrays.copyOfRange(bytes, 4, bytes.length);

            Response deserializedResponse = Protocol.deserialize(bytesWithoutLength);

            assertEquals(emptyResponse, deserializedResponse);
        }
    }

    @Test
    public void serializeErrorResult() throws IOException {
        for (int i = 0; i < 1000000; i++) {
            Response errorResponse = new ErrorResponse("STRING " + i);

            byte[] bytes = Protocol.serialize(errorResponse);
            byte[] bytesWithoutLength = Arrays.copyOfRange(bytes, 4, bytes.length);

            Response deserializedResponse = Protocol.deserialize(bytesWithoutLength);

            assertEquals(errorResponse, deserializedResponse);
        }
    }

    @Test
    public void serializeQueryResult() throws IOException {
        List<List<Constant>> tuples = new ArrayList<>();
        Schema outputSchema = new Schema();
        outputSchema.addIntField("int1", false);
        outputSchema.addStringField("string", 100, false);
        outputSchema.addBooleanField("int2", false);
        outputSchema.addStringField("stringNull", 100, true);

        for (int i = 0; i < 1000000; i++) {
            Constant intConstant = new IntConstant(i);
            Constant stringConstant = new StringConstant("STRING " + i);
            Constant booleanConstant = new BooleanConstant(i % 2 == 0);
            Constant nullConstant = NullConstant.INSTANCE;

            List<Constant> tuple = List.of(intConstant, stringConstant, booleanConstant, nullConstant);

            tuples.add(tuple);
        }

        Response querySet = new QuerySet(outputSchema, tuples);

        byte[] bytes = Protocol.serialize(querySet);
        byte[] bytesWithoutLength = Arrays.copyOfRange(bytes, 4, bytes.length);

        Response deserializedResponse = Protocol.deserialize(bytesWithoutLength);

        assertEquals(querySet, deserializedResponse);
    }
}
