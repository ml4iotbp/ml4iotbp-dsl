package es.upv.pros.ml4iotbp.domain.datasources.process;

import java.util.List;

import es.upv.pros.ml4iotbp.domain.datasources.Variable;

public class ProcessEventDefinition {

    private List<Variable> variables;

    public List<Variable> getVariables() {
        return variables;
    }

    public void setVariables(List<Variable> variables) {
        this.variables = variables;
    }
}
