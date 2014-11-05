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

class Load_Tasks extends LDWH {

    public $retainedKeyColumn;

    function __construct() {
        $this->retainedKeyColumn = 'id';    // This is primary key from the source.
        parent::__construct('Tasks');
        $this->createPLtable();
    }

    function createPLtable() {

        $this->begin('Create PL table');
        $sql = "

CREATE TABLE IF NOT EXISTS  PL_$this->source 
SELECT id, userId, status, activity, completed, updated
FROM ST_$this->source";


        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }
        $stmt->execute();
    }

    function createArchiveTable() {

        $this->begin('CREATE ARCHIVE TABLE');

        $sql = "CREATE TABLE PL_Arch_$this->sourceTable IF NOT EXISTS"
                . " SELECT * FROM PL_$this->sourceTable LIMIT 0";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }

        $stmt->execute();

        $this->end('CREATE ARCHIVE TABLE');
    }

    function addPLcolumns() {
        $this->begin('ALTER PL TABLE');
        $sql = "ALTER TABLE table_name 
        ADD deleted binary(1) DEFAULT 0, 
        ADD PRIMARY KEY (`$this->retainedKeyColumn`)";

        $stmt = $this->ldwhConnection->prepare($sql);

        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }

        $stmt->execute();
        $this->end('ALTER PL TABLE');
    }

    function insert($row) {
        $sql = "INSERT INTO PL_$this->sourceTable
          (id, userId, status, activity, completed,  updated)
          values (?,?,?,?,?, ?)";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === False) {
            print 'error n SQL :' . $this->ldwhConnection->error;
        }

        $stmt->bind_param('iiiiii', $row['id'], $row['userId'], $row['status'], $row['activity'], $row['completed'], $row['updated']);

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
            id, userId, status, activity, completed, updated)
            FROM PL_$this->sourceTable "
                . "WHERE $this->retainedKeyColumn = " . $row['id'];

        $stmt = $this->ldwhConnection->prepare($sql);
        $stmt->store_result();
        $stmt->execute();



        if ($stmt === False) {
            print 'Error in SQL :' . $this->ldwhConnection->error;
        }
        $oldRow = mysqli_fetch_assoc($stmt);

        // We have the current version of the row
        // If the select found a current version, we need to do an update and an archive.
        if (mysqli_num_rows($stmt) > 0) {

            $sql = "UPDATE PL_$this->sourceTable SET
            id = ?, userId = ?, status = ?, activity = ?, completed = ?, updated = ? 
             WHERE ? = ?";
            $archive = true;
        } else {
            $sql = "INSERT INTO PL_$this->sourceTable
            (id, userId, status, activity, completed, updated)
            values (?,?,?,?,?,?)"
                    . " WHERE ? = ?";
            $archive = false;
        }
        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === False) {
            print 'Error in SQL :' . $this->ldwhConnection->error;
        }

        $stmt->bind_param('iiiiiisi', $row['id'], $row['userId'], $row['status'], $row['activity'], $row['completed'], $row['updated'], $this->retainedKeyColumn, // For identifying the record. Use the retained key column 
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
        // This will not affect if the row is the same as before.
        if ($archive && $stmt->affected_rows > 0) {
            // the Row was successfully updated.
            // Now archive the old version
            return $this->archive($oldRow);
        } else {
            return 0;
        }
        // Return number of inserted rows
        return 1;
    }

    /*
     * This simple query performs a very heavy operation, comparing all 
     * the rows in the staging table with all the rows in the current
     * PL layer table. This will detect deleted rows, but if both the source
     * and the PL layer table is too big, this can take a very long time.
     */
    function fetchDeleted(){
        $this->begin('FETCH DELETED');
        
          $sql = "SELECT a.id, b.id from PL_tasks a 
              LEFT JOIN ST_tasks b ON a.id = b.id 
              WHERE b.id IS NULL";
          
        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === False) {
            print 'error in SQL :' . $this->ldwhConnection->error;
        }
        
        
        $stmt->execute();
        $stmt->bind_result($id);
        $deleted = 0;
        while($stmt->fetch()){
           $deleted += $this->delete($id);
        }
        $this->end('FETCH DELETED');
        return $deleted;
    }
    
    /*
     *  Copies the current row into the Arch table and deletes the id from the
     *  PL tables
     */
    function delete($id){
        $sql = "INSERT INTO PL_Arch_$this->sourceTable
        (id, userId, status, activity, completed, name, department, office, updated , deleted) values 
        (SELECT from PL_$this->sourceTable id, userId, status, activity, completed, name, department, office, updated, 1 as deleted WHERE ? = ?)";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === False) {
            print 'error n SQL :' . $this->ldwhConnection->error;
        }
        
        
        $stmt->execute();
        
        
        if ($stmt->errno > 0) {
            switch ($stmt->errno) {
                case 1062:
                    
                    break;
                default :

                    $this->out('INSERT FACT error' . $stmt->error);
                    return $stmt->affected_rows;
                    break;
            }
        }
        return $stmt->insert_id;
        
    }
    
    /*
     * Insert into the archive table an old version of a row.
     * 
     */
        
    function archive($oldRow) {
        $sql = "INSERT INTO PL_Arch_$this->sourceTable
        (id, userId, status, activity, completed,  updated , deleted) values 
        (?,?,?,?,?,?,?)";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === False) {
            print 'error n SQL :' . $this->ldwhConnection->error;
        }
        $deleted = 1;
        $stmt->bind_param('iiiiiii', 
                $oldrow['id'],
                $oldrow['userId'],
                $oldrow['status'],
                $oldrow['activityd'],
                $oldrow['completed'],
                $oldrow['updated'],
                $deleted
                );
        
        $stmt->execute();

        if ($stmt->errno > 0) {
            switch ($stmt->errno) {
                case 1062:
                    // The id already exists in the table. Copy it to the archive table.
                    break;
                default :

                    $this->out('INSERT FACT error' . $stmt->error);
                    return $stmt->affected_rows;
                    break;
            }
        }
        return $stmt->insert_id;
    }

    /*
     * Populate the contents of the dwh
     * 
     * 
     */

    function load() {

        $sql = "SELECT tasks.id, tasks.userId, tasks.status, tasks.activity, tasks.completed,
           updated 
        FROM ST_$this->source";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }

        // bind the results of the query

        $stmt->bind_result(
                $row['id'], $row['userId'], $row['status'], $row['activity'], $row['completed'], $row['updated']
        );

        if ($stmt->errno > 0) {
            print $stmt->error;
        }
        $stmt->execute();
        // For each stamt in the database, perform the ussert on the primary layer.
        while ($stmt->fetch()) {
            $this->upsert($row);
        }
    }

}

?>