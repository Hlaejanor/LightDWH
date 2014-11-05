<?php

/* 
 * LDWB To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

// Load Database variable
const LDWHServer = 'localhost';
const LDWHuser = 'root';
const LDWHPass = 'root';
const LDWHDb = 'LDWH';

require_once 'LDWH.php';

require_once 'Jobs/Tasks/Extract.php';
$E = new Extract_Tasks('01.01.2014');

require_once 'Jobs/Tasks/Load.php';
$L = new Load_Tasks();

require_once 'Jobs/Dimensions/Dimension.php';
require_once 'Jobs/Dimensions/TimeDim.php';
require_once 'Jobs/Dimensions/OrganizationDim.php';
require_once 'Jobs/Tasks/Transform.php';

$T = new Transform_Tasks('TaskCube');

$T->transform();
$T->ensureCubeExists();
$T->populate();

die();


require_once '/LDWH/Highcharts/Chart.php';

?>