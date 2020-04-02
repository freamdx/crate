package io.crate.sql.completion;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SqlCompletionTest {

    private SqlCompletion completion;

    @BeforeEach
    public void setupCompletion() {
        this.completion = new SqlCompletion();
    }

    @Test
    public void test_complete_sel_to_select() throws Exception {
        var candidates = completion.getCandidates("SEL");
        assertThat(candidates, Matchers.contains(is("SELECT")));
    }
}
