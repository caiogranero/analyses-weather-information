# analyses-weather-information

## Hadoop

Instalação do Hadoop Pseudo-distribuido: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

Esse site tem uma explicaço bem detalhada de como funciona o exemplo da Apache de Word Count com Hadoop e MapReduce: http://kickstarthadoop.blogspot.com.br/2011/04/word-count-hadoop-map-reduce-example.html

Para corrigir erros de dependência do Hadoop no eclipse, vá em `Project -> Properties -> Librarys -> Add External JARs` e importe todos os arquivos .jar desses dois diretórios: `/usr/local/hadoop/share/hadoop/common` e `/usr/local/hadoop/share/hadoop/mapreduce`

## Como rodar

1. Primeiro precisa formatar o namenode: `$HADOOP_HOME/bin/hadoop namenode -format`

2. Depois, iniciar o start-dfs.sh: `$HADOOP_HOME/sbin/start-dfs.sh`

3. Para criar uma pasta no hdfs para inserir os inputs: `dfs -mkdir -p /usr/local/hadoop/input` 

4. Copia os arquivos de um path local para o hdfs: `-copyFromLocal /home/caiogranero/workspace/analyses-weather-information/input-teste/* /usr/local/hadoop/input` 

5. Executar .jar `$HADOOP_HOME/bin/hadoop jar /home/caiogranero/workspace/analyses-weather-information/analyses-weather.jar`

6. Visualizar os arquivos de output: `$HADOOP_HOME/bin/hdfs dfs -cat /usr/local/hadoop/output/part-00000`

7. Para gerar um  novo .jar com suas alterações, vá no Eclipse -> Botão direito no projeto -> Export -> Executable Jar

## To-do List

- [ ] Criar interface de usuário
- [ ] Preparar o código para o usuário selecionar o formato de data que será agrupado (Ano > Mês > Dia)
- [ ] Criar outras funções Reduce, sendo uma para cada função estatística (Média, Mediana, Whatever)
- [ ] Implementar o método dos mínimos quadrados
- [ ] Fazer validações de exceções 
- [ ] Relatório
- [ ] . . . 

## Outras coisas

* Listar todas as pastas dentro do input `$HADOOP_HOME/bin/hadoop fs -ls /usr/local/hadoop/input`

