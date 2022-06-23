package twitter_streaming;

import java.util.List;

public class Adjectives {
    private List<String> adj;

    public Adjectives() { };

    public Adjectives(List<String> adj) {
        this.adj = adj;
    }

    public List<String> getAdj() {
        return adj;
    }

    public void setAdj(List<String> adj) {
        this.adj = adj;
    }

    @Override
    public String toString() {
        return adj.toString();
    }
}
