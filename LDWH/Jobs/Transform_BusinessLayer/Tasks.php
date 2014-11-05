<?php

/*
 * TRANSFORM
 * The transform step of the ELT process converts and transforms variables. 
 * It could also be used to
 * combine data from different sources.
 * 
 * The easiest way to understand a Transform layer is to create wide, denormalized temporary tables
 * and then 'normalize'.
 * 
 * 1. Transform - Inplement the transform query. In this particular example it 
 *  explodes a date field into variables containing the constituent time parts.
 * 2. Populate : When you have the variables, you also know which variables they belong in.
 * 
 */

class Transform_Tasks extends LDWH {

    function __construct($cubeName) {
        $this->cubeName = $cubeName;
        parent::__construct('Tasks');
        return $this;
    }

    /*
     * Create the transformation for this transformation into a temporary table
     */

    function transform() {
        $sql = "DROP TABLE T_$this->cubeName;";
           $stmt = $this->ldwhConnection->prepare($sql);
            if($stmt == false){
            print "Error in sql ".$this->ldwhConnection->error;
            die();
        }
        
        $stmt->execute();
        
        $sql ="CREATE TABLE T_$this->cubeName
        SELECT  tasks.id, tasks.userId, tasks.status, tasks.activity, tasks.completed, user.name, user.department, user.office, 
        DAYNAME(tasks.updated) as weekday, DAYOFMONTH(tasks.updated) as dayofmonth, monthname(tasks.updated) as month, QUARTER(tasks.updated) as quarter 
        FROM PL_$this->source 
        INNER JOIN Users ON tasks.userId = user.userId";

        $stmt = $this->ldwhConnection->prepare($sql);
        if($stmt == false){
            print "Error in sql ".$this->ldwhConnection->error;
            die();
        }
        $stmt->execute();
    }
    
    // Ensure that the cube structure exists
    
    
    function ensureCubeExists(){

        $sql = "CREATE TABLE IF NOT EXISTS `BL_$this->cubeName` (
  `id` int(11) unsigned NOT NULL DEFAULT '0',
  `timeDim` int(11) DEFAULT NULL,
  `organizationDim` int(11) DEFAULT NULL,
  `userId` int(11) DEFAULT NULL,
  `status` int(11) DEFAULT NULL,
  `activity` int(11) DEFAULT NULL,
  `completed` int(11) DEFAULT NULL,
  `updated` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;";
        
        $stmt = $this->ldwhConnection->prepare($sql);
       
        if($stmt == false){
            print "Error in sql : ".$this->ldwhConnection->error;
        }
        $stmt->execute();
        if($stmt->errno > 0){
            print "There was an error when creating the DImensionTabe. ".$stmt->error;
            return false;
        }
        return true;
        
    }
    
    /*
     *  Populate the transformation into the Business Layer.
     *  Replace the dimension variables with references to the dimension tables.
     *  Calls Upsert and insertDimensionRow();
     */

    function populate() {
        $sql = "SELECT  id, userId, status, activity, completed, name, department, office, 
        weekday, dayofmonth,  month,  quarter 
        FROM T_$this->cubeName";

        $stmt = $this->ldwhConnection->prepare($sql);
        if($stmt == false){
            print "Error in sql ".$this->ldwhConnection->error;
        }
        $stmt->execute();
        $stmt->store_result();

        // For each variable in the table, find if the variable is a fact (measure)
        // or a dimension variable.
        $timeVariables = array();
        $orgVariables = array();
        $stmt->bind_result($id, $userId, $status, $activity, $completed, $orgVariables['name'], $orgVariables['department'], $orgVariables['office'], $timeVariables['weekday'], $timeVariables['dayofmonth'], $timeVariables['month'], $timeVariables['quarter']);
        // Instantiate Time Dimension
        $timeDim = new TimeDim();

        // Instantiate OrgDim Dimension
        $orgDim = new OrganizationDim();

        while ($stmt->fetch()) {
            // Find the ID of the row that contains all these values
            $timeDimCoordId = $timeDim->getCoord($timeVariables);
            // Find the ID of the row that contains all these values
            $orgDimCoordId = $orgDim->getCoord($orgVariables);
            // At the end of each row, insert/upsert the data into the Business
            $this->upsert($timeDimCoordId, $orgDimCoordId, $id, $userId, $status, $activity);
        }
    }

    function upsert($timeDimCoordId, $orgDimCoordId, $id, $userId, $status, $activity) {

        $sql = "INSERT INTO BL_$this->cubeName
          (organizationDim, timeDim, id, userId, status, activity)
          VALUES (?,?,?,?,?,?)";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === False) {
            print 'error n SQL :' . $this->ldwhConnection->error;
        }
         // This id represents a row containing a unique combination of time variables
        $stmt->bind_param('iiiiii', 
                $orgDimCoordId,
                $timeDimCoordId,
                $id, 
                $userId, 
                $status, 
                $activity
        );

        $stmt->execute();

        if ($stmt->errno > 0) {
            switch ($stmt->errno) {
                case 1062:
                    // The id already exists in the table and this does not allow new entries. We can copy 
                    // update the table, but let us archive the current version first.

                    break;
                default :
                    $this->out('INSERT FACT error' . $stmt->error);
                    return $stmt->affected_rows;
                    break;
            }
        }
    }
 
}

?>