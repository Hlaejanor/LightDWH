<?php

/*
 * EXTRACT 
 * This job extracts data from the Tasks table and
 * inserts a deltaload copy of the data into the staging layer.
 * The Staging Layer does not care about primary Keys, it is a quick and
 * dirty copy operation where nothing should go wrong.
 */
class Extract_Tasks extends LDWH {

    public $beginAt;

    function __construct($beginAt) {

        parent::__construct('Tasks');
        $this->beginAt = $beginAt;
        $this->createStagingTable();
        return true;
    }

    /*
     * Creates a staging table
     */

    function createStagingTable() {
        $this->begin('DELTA LOAD');
        $sql = 
 "DROP TABLE  IF EXISTS ST_$this->source  ;";
           
        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }
        
        $stmt->execute();
        
        /*
        $sql = "CREATE TABLE  ST_$this->source 
SELECT tasks.id, tasks.userId, tasks.status, tasks.activity, tasks.completed, users.name, users.department, users.office, updated 
FROM TASKS 
INNER JOIN users  
ON tasks.userId = users.id  
WHERE updated > '$this->beginAt'";
        */
        
        $sql = "CREATE TABLE  ST_$this->source 
SELECT users.name, users.department, users.office, updated 
FROM USERS 
WHERE updated > '$this->beginAt'";
        
        
        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }
        $stmt->execute();
        if($stmt->errno > 0){
            print $stmt->error;
          
        }
        $this->end('DELTA LOAD');
    }

}

?>