package org.elasticsearch.plugin.gto;

import java.util.List;

/**
 * Created by LJT on 16-11-18.
 * email: linjuntan@sensetime.com
 */
public class ResponseInfo {
    private int cnt;
    private List<List<String>> results;

    public ResponseInfo() {
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    public List<List<String>> getResults() {
        return results;
    }

    public void setResults(List<List<String>> results) {
        this.results = results;
    }
}
