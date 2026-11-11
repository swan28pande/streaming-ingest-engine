package io.github.swan28pande.ingest;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;

import io.github.swan28pande.ingest.proto.IngestRequest;
import io.github.swan28pande.ingest.proto.IngestResponse;
import io.github.swan28pande.ingest.proto.IngestServiceGrpc;
import io.github.swan28pande.ingest.proto.RecordError;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

final class GrpcIngestService extends IngestServiceGrpc.IngestServiceImplBase {
    private final IngestPipeline pipeline;

    GrpcIngestService(IngestPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public void ingest(IngestRequest req, StreamObserver<IngestResponse> resp) {
        try {
            IngestResponse.Builder out = IngestResponse.newBuilder();
            add(out, process(req, 0));
            resp.onNext(out.build());
            resp.onCompleted();
        } catch (Exception e) {
            resp.onError(toStatus(e));
        }
    }

    @Override
    public StreamObserver<IngestRequest> ingestStream(StreamObserver<IngestResponse> resp) {
        return new StreamObserver<>() {
            private final IngestResponse.Builder total = IngestResponse.newBuilder();
            private long index = 0;
            private boolean failed = false;

            @Override
            public void onNext(IngestRequest req) {
                if (failed) return;
                try {
                    add(total, process(req, index));
                    index += req.getRecordsCount();
                } catch (Exception e) {
                    failed = true;
                    resp.onError(toStatus(e));
                }
            }

            @Override
            public void onError(Throwable t) {
                // client cancelled; batches already processed stay published
            }

            @Override
            public void onCompleted() {
                if (failed) return;
                resp.onNext(total.build());
                resp.onCompleted();
            }
        };
    }

    private IngestPipeline.Result process(IngestRequest req, long indexBase) throws IngestPipeline.PublishException {
        Config.Source src = pipeline.source(req.getSource(), Config.Transport.grpc);
        List<JsonNode> records = new ArrayList<>(req.getRecordsCount());
        for (String json : req.getRecordsList()) {
            try {
                records.add(IngestPipeline.JSON.readTree(json));
            } catch (JsonProcessingException e) {
                records.add(NullNode.getInstance()); // reported as invalid by the validator
            }
        }
        return pipeline.ingest(src, Config.Transport.grpc, records, indexBase);
    }

    private static void add(IngestResponse.Builder out, IngestPipeline.Result r) {
        out.setAccepted(out.getAccepted() + r.accepted())
                .setDuplicates(out.getDuplicates() + r.duplicates())
                .setInvalid(out.getInvalid() + r.invalid());
        for (IngestPipeline.RecordError e : r.errors()) {
            out.addErrors(RecordError.newBuilder().setIndex(e.index()).setMessage(e.message()));
        }
    }

    private static io.grpc.StatusRuntimeException toStatus(Exception e) {
        Status s = e instanceof IngestPipeline.UnknownSourceException ? Status.NOT_FOUND
                : e instanceof IngestPipeline.PublishException ? Status.UNAVAILABLE
                : Status.INTERNAL;
        return s.withDescription(e.getMessage()).asRuntimeException();
    }
}
