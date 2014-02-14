/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.planner.node.ESGetNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;
import org.cratedb.Constants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.support.TransportAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ESGetTask implements Task<Object[][]> {

    private final static Visitor visitor = new Visitor();

    private final List<ListenableFuture<Object[][]>> results;
    private final TransportAction transportAction;
    private final ActionRequest request;
    private final ActionListener listener;

    public ESGetTask(TransportMultiGetAction multiGetAction, TransportGetAction getAction, ESGetNode node) {
        assert multiGetAction != null;
        assert getAction != null;
        assert node != null;
        assert node.ids().size() > 0;

        final Context ctx = new Context(node.outputs().size());
        for (Symbol symbol : node.outputs()) {
            visitor.process(symbol, ctx);
        }

        final SettableFuture<Object[][]> result = SettableFuture.create();
        results = Arrays.<ListenableFuture<Object[][]>>asList(result);
        if (node.ids().size() > 1) {
            MultiGetRequest multiGetRequest = new MultiGetRequest();
            for (String id : node.ids()) {
                MultiGetRequest.Item item = new MultiGetRequest.Item(node.index(), Constants.DEFAULT_MAPPING_TYPE, id);
                item.fields(ctx.fields);
                multiGetRequest.add(item);
            }
            multiGetRequest.realtime(true);

            transportAction = multiGetAction;
            request = multiGetRequest;
            listener = new MultiGetResponseListener(result, ctx);
        } else {
            GetRequest getRequest = new GetRequest(node.index(), Constants.DEFAULT_MAPPING_TYPE, node.ids().get(0));
            getRequest.fields(ctx.fields);
            getRequest.realtime(true);

            transportAction = getAction;
            request = getRequest;
            listener = new GetResponseListener(result, ctx);
        }
    }

    static class MultiGetResponseListener implements ActionListener<MultiGetResponse> {

        private final SettableFuture<Object[][]> result;
        private final Context ctx;

        public MultiGetResponseListener(SettableFuture<Object[][]> result, Context ctx) {
            this.result = result;
            this.ctx = ctx;
        }

        @Override
        public void onResponse(MultiGetResponse responses) {
            List<Object[]> rows = new ArrayList<>(responses.getResponses().length);
            for (MultiGetItemResponse response : responses) {
                if (response.isFailed()) {
                    continue;
                }
                Object[] row = new Object[ctx.fields.length];
                int c = 0;
                for (String field : ctx.fields) {
                    row[c] = response.getResponse().getField(field).getValue();
                    c++;
                }
                rows.add(row);
            }

            result.set(rows.toArray(new Object[rows.size()][]));
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    static class GetResponseListener implements ActionListener<GetResponse> {

        private final SettableFuture<Object[][]> result;
        private final Context ctx;

        public GetResponseListener(SettableFuture<Object[][]> result, Context ctx) {
            this.result = result;
            this.ctx = ctx;
        }

        @Override
        public void onResponse(GetResponse response) {
            if (!response.isExists()) {
                result.set(Constants.EMPTY_RESULT);
                return;
            }

            Object[][] rows = new Object[1][ctx.fields.length];
            int c = 0;
            for (String field : ctx.fields) {
                /**
                 * NOTE: mapping isn't applied. So if an Insert was done using the ES Rest Endpoint
                 * the data might be returned in the wrong format (date as string instead of long)
                 *
                 * see {@link org.cratedb.action.parser.SQLResponseBuilder#buildResponse(org.elasticsearch.action.get.GetResponse, long)}
                 * for the old logic
                 *
                 */
                rows[0][c] = response.getField(field).getValue();
                c++;
            }

            result.set(rows);
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void start() {
        transportAction.execute(request, listener);
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException();
    }

    class Context {
        final String[] fields;
        int idx;

        Context(int size) {
            idx = 0;
            fields = new String[size];
        }

        void add(String field) {
            fields[idx] = field;
            idx++;
        }
    }

    static class Visitor extends SymbolVisitor<Context, Void> {

        @Override
        public Void visitReference(Reference symbol, Context context) {
            context.add(symbol.info().ident().columnIdent().fqn());
            return null;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Context context) {
            throw new UnsupportedOperationException();
        }
    }
}
