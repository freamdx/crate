/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.sql.completion;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.AtomTransition;
import org.antlr.v4.runtime.atn.SetTransition;
import org.antlr.v4.runtime.atn.Transition;

import io.crate.sql.parser.CaseInsensitiveStream;
import io.crate.sql.parser.antlr.v4.SqlBaseBaseVisitor;
import io.crate.sql.parser.antlr.v4.SqlBaseLexer;
import io.crate.sql.parser.antlr.v4.SqlBaseParser;

public class SqlCompletion {

    public SqlCompletion() {
    }

    public Iterable<String> getCandidates(String statement) {
        SqlBaseLexer lexer = new SqlBaseLexer(new CaseInsensitiveStream(CharStreams.fromString(statement)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokenStream);
        ArrayList<String> candidates = new ArrayList<>();
        BaseErrorListener listener = new BaseErrorListener() {

            @Override
            public void syntaxError(Recognizer<?, ?> recognizer,
                                    Object offendingSymbol,
                                    int line,
                                    int charPositionInLine,
                                    String msg,
                                    RecognitionException e) {
                if (e == null) {
                    return;
                }
                List<Integer> expectedTokens = e.getExpectedTokens().toList();
                String lastTokenText = e.getOffendingToken().getText();
                Vocabulary vocab = recognizer.getVocabulary();
                for (Integer expectedToken : expectedTokens) {
                    String token = vocab.getSymbolicName(expectedToken);
                    if (token.startsWith(lastTokenText)) {
                        candidates.add(token);
                    }
                }
            }
        };
        lexer.removeErrorListeners();
        lexer.addErrorListener(listener);
        parser.removeErrorListeners();
        parser.addErrorListener(listener);
        var tree = parser.singleStatement();
        tree.accept(new SqlBaseBaseVisitor<>() {
            @Override
            public Object visitTableName(SqlBaseParser.TableNameContext ctx) {
                return super.visitTableName(ctx);
            }
        });
        System.out.println("tokens:");
        for (var token : lexer.getAllTokens()) {
            System.out.println(token);
        }
        var atn = lexer.getATN();
        HashSet<Integer> alreadyPassed = new HashSet<>();
        ArrayList<String> history = new ArrayList<>();
        process(
            SqlBaseLexer.ruleNames,
            lexer.getVocabulary(),
            lexer.getATN().states.get(0),
            alreadyPassed,
            history
        );
        for (var h : history) {
            System.out.println("history:" + h);
        }
        return candidates;
    }

    private void process(String[] ruleNames,
                         Vocabulary vocabulary,
                         ATNState state,
                         HashSet<Integer> alreadyPassed,
                         ArrayList<String> history) {
        if (state.nextTokenWithinRule != null) {
            history.add(state.nextTokenWithinRule.toString(vocabulary));
        }
        for (Transition transition : state.getTransitions()) {
            if (transition.isEpsilon()) {
                if (alreadyPassed.contains(transition.target.stateNumber)) {
                    continue;
                }
                alreadyPassed.add(transition.target.stateNumber);
                process(ruleNames, vocabulary, transition.target, alreadyPassed, history);
            } else if (transition instanceof AtomTransition) {
                AtomTransition atom = (AtomTransition) transition;
                if (alreadyPassed.contains(transition.target.stateNumber)) {
                    continue;
                }
                alreadyPassed.add(transition.target.stateNumber);
                process(ruleNames, vocabulary, atom.target, alreadyPassed, history);
            } else if (transition instanceof SetTransition) {
                SetTransition set = (SetTransition) transition;
                if (alreadyPassed.contains(transition.target.stateNumber)) {
                    continue;
                }
                alreadyPassed.add(transition.target.stateNumber);
                process(ruleNames, vocabulary, set.target, alreadyPassed, history);
            }
        }
    }
}
