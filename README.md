# analyses-weather-information

## Hadoop

Instalação do Hadoop Pseudo-distribuido: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

Esse site tem uma explicaço bem detalhada de como funciona o exemplo da Apache de Word Count com Hadoop e MapReduce: http://kickstarthadoop.blogspot.com.br/2011/04/word-count-hadoop-map-reduce-example.html

Para corrigir erros de dependência do Hadoop no eclipse, vá em `Project -> Properties -> Librarys -> Add External JARs` e importe todos os arquivos .jar desses dois diretórios: `/usr/local/hadoop/share/hadoop/common` e `/usr/local/hadoop/share/hadoop/mapreduce`
