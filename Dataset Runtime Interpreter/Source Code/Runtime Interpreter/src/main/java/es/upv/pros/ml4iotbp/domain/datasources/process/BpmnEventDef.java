package es.upv.pros.ml4iotbp.domain.datasources.process;

import java.util.List;

import es.upv.pros.ml4iotbp.domain.datasources.Variable;

public class BpmnEventDef {

  private String type;
  private List<Variable> variables;
  
  public List<Variable> getVariables() { return variables; }
  public void setVariables(List<Variable> variables) { this.variables = variables; }

  public String getType() { return type; }
  public void setType(String type) { this.type = type; }

}