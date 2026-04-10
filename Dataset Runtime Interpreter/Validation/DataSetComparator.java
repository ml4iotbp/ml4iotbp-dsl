package dataset;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class DataSetComparator {

    public static Integer NUM_COLS=14;

    private static String caseStudy="logistics2";

    public static void main(String[] args) throws IOException{

         InputStream adoc_stream = DataSetComparator.class
                .getClassLoader()
                .getResourceAsStream(caseStudy+"/datasets/ad-hoc.dataset.csv");

            if (adoc_stream == null) {
                throw new RuntimeException("Adhoc dataset File not found in resources");
            }

            InputStream interpreter_stream = DataSetComparator.class
                .getClassLoader()
                .getResourceAsStream(caseStudy+"/datasets/interpreter-dataset.csv");  

            if (interpreter_stream == null) {
                throw new RuntimeException("Interpreter dataset File not found in resources");
            }

         
            String adhoc_dataset[] = new String(adoc_stream.readAllBytes(), StandardCharsets.UTF_8).split("\n");
            String interpreter_dataset[] = new String(interpreter_stream.readAllBytes(), StandardCharsets.UTF_8).split("\n");

            if(adhoc_dataset.length==interpreter_dataset.length){

                HashSet<Integer> different=new HashSet<Integer>();
                HashSet<Integer> no_esta=new HashSet<Integer>();
                for(int i=1;i<adhoc_dataset.length;i++){
                    String adhoc_cols[]=adhoc_dataset[i].split(",");
                    String instance=adhoc_cols[0];
                    for(int j=1;j<interpreter_dataset.length;j++){
                        String interpreter_cols[]=interpreter_dataset[j].split(",");
                        if(interpreter_cols[0].equals(instance)){
                            for(int h=1;h<NUM_COLS-1;h++){
                                if(!interpreter_cols[h].equals(adhoc_cols[h])){
                                    different.add(j);
                                    System.out.println(instance+"=="+interpreter_cols[0]);
                                    System.out.println(i+":"+j+"-->"+adhoc_cols[h]+"=="+interpreter_cols[h]);                         
                                }
                            }
                        }
                    }
                }
                System.out.println("FILAS ANALIZADAS: "+(adhoc_dataset.length-1));
                System.out.println("FILAS IGUALES: "+(adhoc_dataset.length-different.size()-1));
                System.out.print("FILAS INTEPRETE DIFERENTES: ");
                for(Integer i:different){
                    System.out.print((i+1)+" ");
                }
                

             }else{
                System.out.println("Los datasets no coindicen en el número de filas");
            }



    }

}
