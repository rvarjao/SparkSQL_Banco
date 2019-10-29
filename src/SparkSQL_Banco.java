import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class SparkSQL_Banco {
    public static void main(String args[]){
        logger();
        SparkSession session = criaSessao();

        DataFrameReader leitor = criaLeitor(session);

        String[] paths = {
                "in/OMMLBD/ommlbd_basico.csv",
                "in/OMMLBD/ommlbd_empresarial.csv",
                "in/OMMLBD/ommlbd_familiar.csv",
                "in/OMMLBD/ommlbd_regional.csv",
                "in/OMMLBD/ommlbd_renda.csv"};

        ArrayList<Dataset<Row>> datasets = new ArrayList<>();
        for (String path : paths){
            Dataset<Row> dataset = leArquivo(
                    leitor,
                    path,
                    true,
                    true
            );
            datasets.add(dataset);
        }

        Dataset<Row> joined = joinDatasets(datasets, "HS_CPF");
        joined.show();

        realizaTarefas(joined);

    }


    private static void logger(){
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);
    }

    private static SparkSession criaSessao() {
        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("stackoverflow").master("local[2]").getOrCreate();
        return session;
    }

    private static DataFrameReader criaLeitor(SparkSession session) {
        // iniciando leitor de dados
        DataFrameReader leitor = session.read();
        return leitor;
    }

    private static Dataset<Row> leArquivo(DataFrameReader leitor, String path, Boolean header, Boolean inferSchema) {
        Dataset<Row> respostas = leitor
                .option("header", header) //carrega ou nao o cabecalho
                .option("inferSchema", inferSchema) //inferencia de tipos
                .csv(path);
        return respostas;
    }


    public static Dataset<Row> joinDatasets(ArrayList<Dataset<Row>> datasets, String joinColumn){
        Dataset<Row> joined = datasets.get(0);
        int i = 0;
        for (Dataset<Row> dataset : datasets){
            if (i == 0){
                i++;
                continue;
            }
            joined = joined.join(dataset, joinColumn);
        }

        return joined;
    }

    public static void realizaTarefas(Dataset<Row> dataset){
        dataset.show();
    }

}