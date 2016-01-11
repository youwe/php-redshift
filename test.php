#! /usr/local/bin/php
<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2016-01-11
 * Time: 17:52
 */

require_once 'vendor/autoload.php';

use Oasis\Mlib\AwsWrappers\StsClient;
use Oasis\Mlib\Redshift\RedshiftConnection;

$rs = RedshiftConnection::getConnection(
    [
        "host"     => "***",
        "port"     => 5439,
        "dbname"   => "dmp",
        "user"     => "dmp",
        "password" => "***",
    ]
);

$stmt = <<<SQL
select events.event_id, events.appid, version, events.uuid, "timestamp", p1.v, p2.v from events left join params p1 on events.uuid = p1.uuid and events.event_id = p1.event_id and p1.k = 'os'
left join params p2 on events.uuid = p2.uuid and events.event_id = p2.event_id and p2.k = 'locale'
SQL;

$s3path = "s3://brotsoft-dmp/test2-export";

$sts = new StsClient([
    "profile" => "dmp-user",
    "region" => 'us-east-1'
]);
$credential = $sts->getTemporaryCredential();

$rs->unloadToS3($stmt, $s3path, $credential, true, true);

$columns = "a1,a2,a3,a4,a5,a6,a7";
$rs->copyFromS3("test", $columns, $s3path, 'us-east-1', $credential, true, true);
