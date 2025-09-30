options nocenter;
options threads cpucount=16;
options sortsize=64G;
options sumsize=64G;

/* libname slui '/sasdata/F/ctirss/sasdatos/Luis/'; */
libname slu '/sasdata/F/ctirss/sasdatos/Compdata/PTFinales/';
libname outlib '/sasdata/F/ctirss/sasdatos/Esteban/'; /* Renamed libref */

/* Define the directory containing the files */
%let directory = /sasdata/F/ctirss/sasdatos/Compdata/PTFinales/;

/* Use FILENAME PIPE to list files matching the pattern */
filename dirlist pipe "ls &directory.pafinalmes*";

data file_list;
    infile dirlist truncover;
    input filename $256.;
    /* Extract the file name without the directory path */
    full_periodo = scan(filename, -1, '/');
    /* Remove files that end as ant.sas7bdat */
    if substr(full_periodo, length(full_periodo)-11) = 'ant.sas7bdat' then delete;
    /* Remove the .sas7bdat extension to get the dataset name */
    periodo = scan(full_periodo, 1, '.');
run;

/* Keep only the latest periodo */
proc sql;
    create table file_list as
    select *
    from file_list
    having periodo = max(periodo);
quit;

/* Show contents of file_list for debugging */
proc print data=file_list noobs;
run;

/* Macro to operate on the resulting dataset */
%macro process_employment_data();
    /* Get the dataset name from file_list */
    proc sql noprint;
        select periodo into :dataset_name
        from file_list;
    quit;
    
    %put NOTE: Processing dataset: &dataset_name;
    
    /* Load the dataset using the identified period */
    data work.employment_data;
        set slu.&dataset_name;
    run;
    
    proc sql;
        create table work.employment_counts as
        select 
            ta,
            div_final,
            size_cierre,
            cve_del_final,
            cve_subdel_final,
            mod,
            tipotrc,
            count(nss) as asegurados,
            mean(sal_cierre ) as salario,
            sum(empresas3) as patrones
        from work.employment_data
        where aseg_cierre = 1 
            and aseg = 1
        group by ta, cve_del_final, cve_subdel_final, mod, tipotrc, div_final, size_cierre
        order by ta, cve_del_final, cve_subdel_final, mod, tipotrc, div_final, size_cierre;
    quit;
    
    /* Display the first rows of the dataset for verification */
    proc print data=work.employment_counts (obs=10);
        title "First 10 Rows of Employment Counts";
    
%mend process_employment_data;

/* Execute the macro */
%process_employment_data();

/* Save the final dataset to the output library */
data outlib.employment_counts;
    set work.employment_counts;
run;
