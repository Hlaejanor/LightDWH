<?php

/*
 * LOAD 
 * This job loads data from the staging layer into the Primary Layer.
 * The Primary Layer contains history, which no other layers normally do.
 * Therefore, the Class will create two different tables, one currenttable
 * and one Archivetable. The archivetable is used to save a copy.
 * 
 * 1. CreatePLtable - Creates a PL table if not exists
 * 2. CreateArchiveTable - Create an PL archive table if not exists
 * 3. AddPLcolumns - Adds additional PL columns such as the deleted flag and sets the PrimaryKey to the retainedKey Column
 *
 * 
 * The current PHP implementation is not very efficient, particularly, the upsert function
 * with unesccesary piping between the PHP preprocessor and the language. The upsert function
 * can be implemented as stored MySQL procedures for far higher peformance. 
 * 
 */

class Load_Users extends LDWH {

    public $retainedKeyColumn;

    function __construct() {
        $this->retainedKeyColumn = 'userId';    // This is primary key from the source.
        parent::__construct('User');
        $this->createPLtable();
    }

    function createPLtable() {

        $this->begin('Create PL table');
        $sql = "

CREATE TABLE IF NOT EXISTS  PL_$this->source 
SELECT  userId,  completed, name, department, office, updated
FROM ST_$this->source";
        

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }
        $stmt->execute();
    }

    function createArchiveTable() {

        $this->begin('Create PL table');

        $sql = "CREATE TABLE PL_Arch_$this->sourceTable IF NOT EXISTS"
                . " SELECT * FROM PL_$this->sourceTable LIMIT 0";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }

        $stmt->execute();

        $this->end('DELTA LOAD');
    }

    function addPLcolumns() {
        $sql = "ALTER TABLE table_name 
        ADD deleted binary(1) DEFAULT 0, 
        ADD PRIMARY KEY (`$this->retainedKeyColumn`)";

        $stmt = $this->ldwhConnection->prepare($sql);

        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }

        $stmt->execute();
    }

    function insert($row) {
        $sql = "INSERT INTO PL_$this->sourceTable
          ( userId, name, department, office )
          values (?,?,?,?)";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === False) {
            print 'error n SQL :' . $this->ldwhConnection->error;
        }
       
        $stmt->bind_param('isss',
                $row['id'],
                $row['name'],
                $row['department'],
                $row['office']
            );

        $stmt->execute();

        if ($stmt->errno > 0) {
            switch ($stmt->errno) {
                case 1062:
                    // The id already exists in the table and this does not allow new entries. We can copy 
                    // update the table, but let us archive the current version first.
                    return $this->update($row);
                    break;
                default :
                    $this->out('INSERT FACT error' . $stmt->error);
                    return $stmt->affected_rows;
                    break;
            }
        }

        /*
         * Takes the current version of a row and saves it in the archive table.
         * 
         */

        
       
    }

    /*
     * Updating a row
     * 1. Get the old version of the PL table entry
     * 2. Update the PL table entry.
     * 3. If there is any change in the PL table, the old version is archived in the Arch table
     */

    function upsert($row) {
        
        $sqlSelect = "SELECT 
           userId, name, department, office, )
            FROM PL_$this->sourceTable "
            . "WHERE $this->retainedKeyColumn = ".$row['id'];
        
        $stmt = $this->ldwhConnection->prepare($sql);
        $stmt->store_result();
        $stmt->execute();
        
        
        
        if ($stmt === False) {
            print 'Error in SQL :' . $this->ldwhConnection->error;
        }
        $row = mysqli_fetch_assoc($stmt);
        
        // We have the current version of the row
        
        $sql = "UPDATE PL_$this->sourceTable
            (userId, name, department, office )
            values (?,?,?,?,?,?)"
            . " WHERE ? = ?";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === False) {
            print 'Error in SQL :' . $this->ldwhConnection->error;
        }

        $stmt->bind_param('iiiisi',
               
                $row['userId'], 
                $row['name'],
                $row['department'],
                $row['office'],
                $this->retainedKeyColumn, // For identifying the record. Use the retained key column 
                $row['id']);                // And the retained key

        $stmt->execute();

        if ($stmt->errno > 0) {
            switch ($stmt->errno) {

                default :

                    $this->out('UPDATE PL RECORD error' . $stmt->error);
                    return $stmt->affected_rows;
                    break;
            }
        }
        if ($stmt->affected_rows > 0){
            // the Row was successfully updated.
            // Now archive the old version
           return $this->archive($row);
        }
        else{
            return 0;
        }
    }
    
    
    /*
     * Insert into the archive table an old version
     * 
     */
    
    function archive($id) {
            $sql = "INSERT INTO PL_Arch_$this->sourceTable
        (userId, name, department, office, updated, deleted ) values 
        (SELECT from PL_$this->sourceTable userId, name, department, office,  updated, 1 as deleted WHERE ? = ?)";

            $stmt = $this->ldwhConnection->prepare($sql);
            if ($stmt === False) {
                print 'error n SQL :' . $this->ldwhConnection->error;
            }
            $stmt->bind_param('si', $this->retainedKeyColumn, $id);

            $stmt->execute();

            if ($stmt->errno > 0) {
                switch ($stmt->errno) {
                    case 1062:
                        // The id already exists in the table. Copy it to the archive table.
                        //return  $this->update($domain, $subDomain, $URL, $relevance, $urlRelevance, $humanRelevance, $content);
                        break;
                    default :

                        $this->out('INSERT FACT error' . $stmt->error);
                        return $stmt->affected_rows;
                        break;
                }
            }
             return $stmt->insert_id;
        }
        
        function populate(){
      
        $sql = "SELECT userId, name, department, office, 
           updated 
        FROM ST_".$this->source;
                     
        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }
     
        
        $stmt->bind_result(
                $row['userId'],
                $row['name'], 
                $row['department'], 
                $row['office']
                );
        
        if($stmt->errno > 0){
            print $stmt->error;
        }
           $stmt->execute();
        
           
           while ($stmt->fetch()){
               $this->upsert($row);
           }
            
        }
}

?>