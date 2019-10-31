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
//        questao03(dataset);
//        questao04(dataset);
//        contagem(dataset);

//        questao05(dataset);
//        questao06(dataset);
//        questao07(dataset);
        questao08(dataset);

    }



    public static void contagem(Dataset<Row> dataset) {
        System.out.println("Contagem de HS_CPF de toda a base");
        dataset = removeInconsistencias(dataset, "HS_CPF");

        Dataset<Row> hs_cpf = dataset.agg(count(col("HS_CPF")).alias("ContagemCPF"));
        hs_cpf.show();
        salvaDataSetEmArquivo(hs_cpf, "output/banco/contagem.csv", 1);
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

    public static void questao04(Dataset<Row> dataset) {
        System.out.println("O número de propostas de crédito cujo cliente é beneficiário do bolsa família (BOLSAFAMILIA)");
        dataset = removeInconsistencias(dataset, "BOLSAFAMILIA");

        Dataset<Row> datasetBolsaFamilia = dataset.filter(col("BOLSAFAMILIA").equalTo(1));
        Dataset<Row> hs_cpf = datasetBolsaFamilia.agg(count(col("HS_CPF")));
        hs_cpf.show();
        salvaDataSetEmArquivo(hs_cpf, "output/banco/questao04-bolsa_familia.csv", 1);
    }

    public static void questao05(Dataset<Row> dataset){
        System.out.println("O percentual de propostas de crédito cujo cliente possui um funcionário público em casa" +
                " (use a coluna FUNCIONARIOPUBLICOCASA)");

        //como deve ser tratado? Tirar agora ira reduzir o numero maximo na contagem de total
//        dataset = removeInconsistencias(dataset, "FUNCIONARIOPUBLICOCASA");

        try {
            dataset.createTempView("df");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }finally {
            SQLContext sqlContext = dataset.sqlContext();
            String strSQL = "SELECT count(HS_CPF)/(SELECT count(*) FROM df) FROM df WHERE FUNCIONARIOPUBLICOCASA = 1";
            Dataset<Row> datasetFuncionarioPublico = sqlContext.sql(strSQL);

            datasetFuncionarioPublico.show();
            salvaDataSetEmArquivo(datasetFuncionarioPublico, "output/banco/questao05-datasetFuncionarioPublico.csv", 1);
        }

    }

    public static void questao06(Dataset<Row> dataset){
        System.out.println("O percentual de propostas de crédito cujo cliente vive em uma cidade com IDH em cada uma das faixas: " +
                "0 a 10, 10 a 20, 20 a 30, 30 a 40, 40 a 50, 50 a 60, 60 a 70, 70 a 80" +
                "80 a 90 e 90 a 100;");

        //como deve ser tratado? Tirar agora ira reduzir o numero maximo na contagem de total
        //        dataset = removeInconsistencias(dataset, "IDHMUNICIPIO");

        try {
            dataset.createTempView("df");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }finally {
            SQLContext sqlContext = dataset.sqlContext();

            String strSQL = " SELECT ( (floor(IDHMUNICIPIO / 10) * 10) ) as FAIXA_IDH," +
                    " count(HS_CPF) as CONTAGEM, count(HS_CPF)/(SELECT count(*) FROM df) as PORCENTAGEM " +
                    " FROM df " +
                    " GROUP BY FAIXA_IDH " +
                    " ORDER BY FAIXA_IDH";

            Dataset<Row> datasetIDH = sqlContext.sql(strSQL);
            datasetIDH.show();

            datasetIDH.printSchema();

            datasetIDH.show();
            salvaDataSetEmArquivo(datasetIDH, "output/banco/questao06-AGRUPADO_IDH.csv", 1);
        }
    }

    public static void questao07(Dataset<Row> dataset){
        System.out.println("O número de propostas de clientes que vivem próximos de uma região de risco " +
                "(isto é, cuja distância para a zona de risco mais próxima é menor que 5km) " +
                "e que possuam renda maior que R$ 7.000,00 (Sete mil reais)");

                dataset = removeInconsistencias(dataset, "DISTZONARISCO");
                dataset = removeInconsistencias(dataset, "ESTIMATIVARENDA");

        try {
            dataset.createTempView("df");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }finally {
            SQLContext sqlContext = dataset.sqlContext();

            String strSQL = " SELECT count(HS_CPF) as CLIENTES_EM_ZONA_DE_RISCO " +
                    " FROM df " +
                    " WHERE DISTZONARISCO < 5000 AND ESTIMATIVARENDA > 7000";

            Dataset<Row> datasetIDH = sqlContext.sql(strSQL);
            datasetIDH.show();

            datasetIDH.printSchema();

            salvaDataSetEmArquivo(datasetIDH, "output/banco/questao07-ZONA_RISCO.csv", 1);
        }
    }

    public static void questao08(Dataset<Row> dataset){
        System.out.println("O número de propostas de clientes adimplentes e inadimplentes (TARGET) " +
                "que possuem renda maior que R$ 5.000,00 (cinco mil reais) e " +
                "também se eles são sócios de empresas ou não (SOCIOEMPRESA)");

        dataset = removeInconsistencias(dataset, "ESTIMATIVARENDA");
        dataset = removeInconsistencias(dataset, "SOCIOEMPRESA");

        try {
            dataset.createTempView("df");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }finally {
            SQLContext sqlContext = dataset.sqlContext();

            String strSQL = " SELECT TARGET, SOCIOEMPRESA, count(HS_CPF) as NUMERO_CLIENTES" +
                    " FROM df " +
                    " WHERE ESTIMATIVARENDA > 5000" +
                    " GROUP BY TARGET, SOCIOEMPRESA " +
                    " ORDER BY TARGET, SOCIOEMPRESA ";

            Dataset<Row> datasetIDH = sqlContext.sql(strSQL);
            datasetIDH.show();

            datasetIDH.printSchema();

            salvaDataSetEmArquivo(datasetIDH, "output/banco/questao08-TARGET.csv", 1);
        }
    }

}