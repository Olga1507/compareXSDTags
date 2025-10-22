package ru.diasoft.comparexsdtags.model;
import java.util.List;

public class ValidationResult {
    private boolean valid;
    private List<String> differences;

    public ValidationResult(boolean valid, List<String> differences) {
        this.valid = valid;
        this.differences = differences;
    }

    // Getters
    public boolean isValid() { return valid; }
    public List<String> getDifferences() { return differences; }
}
