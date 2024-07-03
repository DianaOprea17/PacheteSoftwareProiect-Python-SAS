/*Crearea unui set de date SAS din fișiere externe.
*/
LIBNAME mylib '/home/u63833483/Proiect2';

DATA mylib.dataset_sas; 
    INFILE '/home/u63833483/Proiect2/dataset1.csv' DLM=',' FIRSTOBS=2; 
    INPUT         
        ID Loan_Amount Funded_Amount Funded_Amount_Investor Interest_Rate Adjusted_Rate
        Employment_Duration Debit_to_Income Open_Account Revolving_Balance Revolving_Utilities
        Total_Accounts Total_Received_Interest Total_Received_Late_Fee Recoveries
        Collection_Recovery_Fee Collection_12_months_Medical Last_week_Pay Accounts_Delinq
        Total_Collection_Amount Total_Current_Balance Total_Savings_Balance Total_Revolving_Credit_Limit
        Total_annual_fees Days_since_last_inflow $ Days_since_last_outflow Grade $1. Home_Ownership $
        Verification_Status $ Loan_Category $ Initial_List_Status $ Application_Type $ Internal_Customer_Segment $
        Loan_Status $ Payment_Plan $ Sub_Grade $;
RUN;
PROC EXPORT DATA=mylib.dataset_sas
            OUTFILE='/home/u63833483/Proiect2/dataset_sas.csv'
            DBMS=CSV REPLACE;
RUN;

/*Modificați setul de date astfel încât să aplicați un format definit de utilizator 
pentru coloana Grade, transformând valorile 'A' în 'Excelent', 'B' în 'Foarte Bun', 
'C' în 'Bun', 'D' în 'Acceptabil', 'E' în 'Mediu', 'F' în 'Slab' și 'G' în 'Foarte Slab'.
*/
PROC FORMAT;
    VALUE $grade_fmt
        'A' = 'Excelent'
        'B' = 'Foarte Bun'
        'C' = 'Bun'
        'D' = 'Acceptabil'
        'E' = 'Mediu'
        'F' = 'Slab'
        'G' = 'Foarte Slab'
        ;
RUN;

DATA mylib.dataset_sas;
    SET mylib.dataset_sas;
    Grade = PUT(Grade, $grade_fmt.);
RUN;

PROC EXPORT DATA=mylib.dataset_sas
            OUTFILE='/home/u63833483/Proiect2/dataset_sas.csv'
            DBMS=CSV REPLACE;
RUN;

PROC PRINT DATA=mylib.dataset_sas (OBS=10);
    FORMAT Grade $grade_fmt.;
RUN;



/*Creați un nou set de date care să includă doar observațiile pentru care 
Debit_to_Income este mai mic de 20. În acest nou set de date, să se calculeze:
       a)Total_Received_Interest să fie mărit cu 5% dacă Total_Accounts este mai mare de 10. 
       Dacă Open_Account este mai mare de 5, Total_Received_Late_Fee să fie redus cu 5%.
       b)Creșteți Revolving_Utilities cu 5 până când atinge valoarea de 70.
       c)Calculați un nou indicator, New_Indicator, ca diferența dintre 
       Total_Current_Balance și Total_Savings_Balance, împărțită la Total_Revolving_Credit_Limit.
*/
DATA mylib.dataset_processed;
    SET mylib.dataset_sas;

    WHERE Debit_to_Income < 20;

    IF Total_Accounts > 10 THEN DO;
        Total_Received_Interest = Total_Received_Interest * 1.05;
        IF Open_Account > 5 THEN DO;
            Total_Received_Late_Fee = Total_Received_Late_Fee * 0.95;
        END;
    END;
    DO UNTIL(Revolving_Utilities >= 70);
        Revolving_Utilities = Revolving_Utilities + 5;
    END;

    New_Indicator = (Total_Current_Balance - Total_Savings_Balance) / Total_Revolving_Credit_Limit;

RUN;

PROC EXPORT DATA=mylib.dataset_processed 
            OUTFILE='/home/u63833483/Proiect2/dataset_processed.sas7bdat' 
            DBMS=CSV;
RUN;

PROC PRINT DATA=mylib.dataset_processed (OBS=10);
    VAR ID Total_Accounts Revolving_Utilities Open_Account Debit_to_Income 
        Total_Received_Interest Total_Received_Late_Fee New_Indicator;
    FORMAT New_Indicator percent7.4;  
RUN;

/*Realizați un subset de date. Subsetul trebuie să includă doar observațiile 
pentru care valoarea din coloana Total_Accounts este mai mare de 20. 
Din acest subset, selectați și păstrați doar următoarele coloane: 
ID, Total_Accounts, Revolving_Utilities, Open_Account, Debit_to_Income, 
Total_Received_Interest, și Total_Received_Late_Fee.
*/

DATA mylib.dataset_subset2;
    SET mylib.dataset_sas;
    
    RETAIN Total_Accounts_numeric;
    Total_Accounts_numeric = input(Total_Accounts, 8.); 
    WHERE Total_Accounts_numeric > 20;
    KEEP ID Total_Accounts_numeric Revolving_Utilities Open_Account Debit_to_Income
         Total_Received_Interest Total_Received_Late_Fee;
    DROP Total_Accounts_numeric;
RUN;

PROC PRINT DATA=mylib.dataset_subset2 (OBS=10);
    VAR ID Total_Accounts_numeric Revolving_Utilities Open_Account Debit_to_Income 
        Total_Received_Interest Total_Received_Late_Fee;
RUN;


/*Folosind funcții SAS, adăugați o nouă variabilă, Below_Avg_Interest_Rate, 
care să fie egală cu 1 dacă Interest_Rate este mai mică decât media generală 
a ratelor dobânzilor din setul de date, și 0 în caz contrar. 
Calculați Total_Credit_Balance ca suma dintre Total_Current_Balance și 
Total_Savings_Balance. De asemenea, adăugați o variabilă, High_Accounts, 
care să fie egală cu 1 dacă Total_Accounts este mai mare decât 10, și 0 în caz contrar.
*/

PROC SQL NOPRINT;
    SELECT AVG(Interest_Rate) INTO :avg_interest_rate FROM mylib.dataset_sas;
QUIT;
DATA mylib.dataset_subset3;
    SET mylib.dataset_sas;
    Below_Avg_Interest_Rate = IFN(Interest_Rate < &avg_interest_rate, 1, 0);
    
    Total_Credit_Balance = SUM(Total_Current_Balance, Total_Savings_Balance);
    
    High_Accounts = IFN(Total_Accounts > 10, 1, 0);
    
    KEEP ID Total_Accounts  
         Total_Current_Balance Total_Savings_Balance Total_Credit_Balance 
         High_Accounts Below_Avg_Interest_Rate;
RUN;

PROC PRINT DATA=mylib.dataset_subset3 (OBS=10);
    VAR ID Total_Accounts  
         Total_Current_Balance Total_Savings_Balance Total_Credit_Balance 
         High_Accounts Below_Avg_Interest_Rate;
   
RUN;

/*Creați 3 rapoarte conținând coloanele
ID, Open Accounts, Loan Category, Verification Status, fiecare raport fiind grupat după starea 
în care se află: Verified, Not Verified, Source Verified.*/

DATA mylib.Verified;
    SET mylib.dataset_sas (keep=ID Open_Account Loan_Category Verification_Status);
    WHERE Verification_Status = "Verified";
RUN;

PROC REPORT DATA=Verified (obs=10) nowd;
    COLUMN ID Open_Account Verification_Status Loan_Category;
    DEFINE ID / GROUP;
    DEFINE Open_Account / SUM "Open Accounts";
    DEFINE Verification_Status / GROUP "Verification Status";
    DEFINE Loan_Category / GROUP "Loan Category";
    TITLE "Raport - Verified";
RUN;

DATA mylib.NotVerified;
    SET mylib.dataset_sas (keep=ID Open_Account Loan_Category Verification_Status);
    WHERE Verification_Status = "Not Veri";
RUN;

PROC REPORT DATA=NotVerified (obs=10) nowd;
    COLUMN ID Open_Account Verification_Status Loan_Category;
    DEFINE ID / GROUP;
    DEFINE Open_Account / SUM "Open Accounts";
    DEFINE Verification_Status / GROUP "Verification Status";
    DEFINE Loan_Category / GROUP "Loan Category";
    TITLE "Raport - NotVerified";
RUN;

DATA mylib.SourceVerified;
    SET mylib.dataset_sas (keep=ID Open_Account Loan_Category Verification_Status);
    WHERE Verification_Status = "Source V";
RUN;

PROC REPORT DATA=SourceVerified (obs=10) nowd;
    COLUMN ID Open_Account Verification_Status Loan_Category;
    DEFINE ID / GROUP;
    DEFINE Open_Account / SUM "Open Accounts";
    DEFINE Verification_Status / GROUP "Verification Status";
    DEFINE Loan_Category / GROUP "Loan Category";
    TITLE "Raport - SourceVerified";
RUN;

/*Combinați subseturile Verified, NotVerified și SourceVerified 
într-un singur set de date folosind proceduri specifice SQL*/
PROC SQL;
    CREATE TABLE mylib.VerifCombined AS
    SELECT ID, Open_Account, Loan_Category, Verification_Status
    FROM mylib.Verified
    UNION ALL
    SELECT ID, Open_Account, Loan_Category, Verification_Status
    FROM mylib.NotVerified
    UNION ALL
    SELECT ID, Open_Account, Loan_Category, Verification_Status
    FROM mylib.SourceVerified
    ORDER BY Open_Account;
QUIT;


PROC PRINT DATA=mylib.VerifCombined (OBS=10);
RUN;

/*Folosind masive, calculați numărul total de conturi deschise din cele 3 subseturi: 
Verified, NotVerified, SourceVerified.*/

DATA mylib.TotalOpenAccounts;
    Total_Open_Account = 0;
    ARRAY Open_Accounts_Sum[3] (0,0,0);
    DO i = 1 TO 3;
        IF i = 1 THEN DO;
            DO WHILE(NOT End1);
                SET mylib.Verified (keep=Open_Account) END=End1;
                Open_Accounts_Sum[1] = Open_Accounts_Sum[1] + Open_Account;
            END;
        END;
        ELSE IF i = 2 THEN DO;
            DO WHILE(NOT End2);
                SET mylib.NotVerified (keep=Open_Account) END=End2;
                Open_Accounts_Sum[2] = Open_Accounts_Sum[2] + Open_Account;
            END;
        END;
        ELSE IF i = 3 THEN DO;
            DO WHILE(NOT End3);
                SET mylib.SourceVerified (keep=Open_Account) END=End3;
                Open_Accounts_Sum[3] = Open_Accounts_Sum[3] + Open_Account;
            END;
        END;
    END;
    Total_Open_Account = sum(of Open_Accounts_Sum[*]);
    DROP i Open_Account End1 End2 End3;
    OUTPUT;
    STOP; 
RUN;

PROC PRINT DATA=mylib.TotalOpenAccounts;
RUN;


/*Să se folosească procedura RANK pentru a clasifica conturile deschise (Open_Account) din setul de date. */

PROC RANK DATA=mylib.dataset_sas OUT=mylib.RankedOpenAccounts;
    VAR Open_Account;
    RANKS Rank_Open_Account;
RUN;

PROC PRINT DATA=mylib.RankedOpenAccounts (OBS=20);
    TITLE "Ranked Open Accounts";
RUN;

PROC PRINT DATA=mylib.VerifCombined (OBS=10);
RUN;


/* Generati o diagramă de corelație între Loan Amount și Funded Amount utilizând GPLOT */
PROC GPLOT DATA=mylib.dataset_sas;
    PLOT Loan_Amount * Funded_Amount / HAXIS=AXIS1 VAXIS=AXIS2;
    TITLE "Diagrama de corelatie intre Loan Amount si Funded Amount";
    TITLE2 "Densitatea marginala a variabilelor";
RUN;
