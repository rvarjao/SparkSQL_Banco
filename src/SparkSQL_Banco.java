import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkSQL_Banco {
    public static void main(String args[]){

        logger();
        SparkSession session = criaSessao();
        DataFrameReader reader = criaLeitor(session);

        Dataset<Row> dataset = AjustaDados(reader);

        realizaTarefas(dataset);

    }


    private static Dataset<Row> AjustaDados(DataFrameReader reader){

        String[] paths = {
                "in/OMMLBD/ommlbd_basico.csv",
                "in/OMMLBD/ommlbd_empresarial.csv",
                "in/OMMLBD/ommlbd_familiar.csv",
                "in/OMMLBD/ommlbd_regional.csv",
                "in/OMMLBD/ommlbd_renda.csv"};

        ArrayList<Dataset<Row>> datasets = new ArrayList<>();
        for (String path : paths){
            Dataset<Row> dataset = leArquivo(
                    reader,
                    path,
                    true,
                    true
            );

            datasets.add(dataset);
        }

        Dataset<Row> joined = joinDatasets(datasets, "HS_CPF");
//        joined.show();

        // nao ha mais a necessidade de fazer a validacao
        // pois ja foi realizada
//        validaDados(joined);

        return joined;

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
        String[] comps = path.split("/");
        String alias = comps[comps.length - 1].replace(".csv", "");

        Dataset<Row> respostas = leitor
                .option("header", header) //carrega ou nao o cabecalho
                .option("inferSchema", inferSchema) //inferencia de tipos
                .csv(path)
                .alias(alias);
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

    public static Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public static void validaDados(Dataset<Row> dataset){
        // calculcar a media de cada coluna
        RelationalGroupedDataset groupedDataset = dataset.groupBy(col("SAFRA"));

        String[] columns = dataset.columns();

        ArrayList<String> listColumns = new ArrayList<>();
        listColumns.addAll(Arrays.asList(columns));

        Tuple2<String, String>[] dtypes = dataset.dtypes();
        for (Tuple2<String, String> dtype : dtypes){
            if (dtype._2().equals("StringType")){
                listColumns.remove(dtype._1());
            }
        }

        Seq<String> stringSeq = convertListToSeq(listColumns);

        Dataset<Row> aggAvg = groupedDataset.avg(stringSeq);
        aggAvg.show();
    }

    public static Dataset<Row> removeInconsistencias(Dataset<Row> dataset, String column){

        // calculcar a media de cada coluna

        dataset = dataset.filter(col(column).notEqual("-9999"));
        dataset = dataset.filter(col(column).notEqual("-9998"));

        return dataset;
    }

    public static void salvaDataSetEmArquivo(Dataset<Row> dataset, String path, int nOutputFiles){
        dataset.coalesce(nOutputFiles)
                .write()
                .option("header","true")
                .option("delimiter", ",")
                .mode("overwrite")
                .format("csv")
                .save(path);
    }

    // --=== QUESTOES ===--
    public static void realizaTarefas(Dataset<Row> dataset){
//        questao01(dataset);
//        questao02(dataset);
        questao03(dataset);
    }

    public static void questao01(Dataset<Row> dataset) {
        System.out.println("O número de clientes por orientação sexual");
        dataset = removeInconsistencias(dataset, "ORIENTACAO_SEXUAL");
        RelationalGroupedDataset groupedDataset = dataset.groupBy(col("ORIENTACAO_SEXUAL"));

        Dataset<Row> contagemOrientacaoSexual = groupedDataset.agg(count(col("HS_CPF").alias("NumeroDeClientes")));
        contagemOrientacaoSexual.show();
        salvaDataSetEmArquivo(contagemOrientacaoSexual, "output/banco/count_orientacaoSexual.csv", 1);
    }


    public static void questao02(Dataset<Row> dataset) {
        System.out.println("O número máximo e mínimo de e-mails cadastrados em toda a base de dados");
        dataset = removeInconsistencias(dataset, "QTDEMAIL");

        Dataset<Row> datasetEmail = dataset.agg(max(col("QTDEMAIL")), min(col("QTDEMAIL")));
        datasetEmail.show();
        salvaDataSetEmArquivo(datasetEmail, "output/banco/max_min_emails.csv",1);

    }

    public static void questao03(Dataset<Row> dataset) {
        System.out.println("O número de propostas cujo cliente possui estimativa de renda " +
                "superior a R$ 10.000,00 (dez mil reais)");

        dataset = removeInconsistencias(dataset, "ESTIMATIVARENDA");

        Dataset<Row> datasetEstimativa = dataset.filter(col("ESTIMATIVARENDA").$greater(10000));
        Dataset<Row> hs_cpf = datasetEstimativa.agg(count(col("HS_CPF")));
        hs_cpf.show();
        salvaDataSetEmArquivo(hs_cpf, "output/banco/renda_superior_10000.csv", 1);
    }

    



}