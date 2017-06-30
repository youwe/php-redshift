#! /usr/local/bin/php
<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2016-01-11
 * Time: 17:52
 */

require_once 'vendor/autoload.php';

use Oasis\Mlib\AwsWrappers\S3Client;
use Oasis\Mlib\AwsWrappers\StsClient;
use Oasis\Mlib\FlysystemWrappers\ExtendedAwsS3Adapter;
use Oasis\Mlib\FlysystemWrappers\ExtendedFilesystem;
use Oasis\Mlib\FlysystemWrappers\ExtendedLocal;
use Oasis\Mlib\Redshift\DrdStreamWriter;
use Oasis\Mlib\Redshift\RedshiftConnection;
use Oasis\Mlib\Redshift\RedshiftExporter;
use Oasis\Mlib\Redshift\RedshiftImporter;
use Oasis\Mlib\Redshift\StsCredentialProvider;

$awsConfig = [
    'profile' => "oasis-minhao",
    'region'  => 'ap-northeast-1',
];

$sts      = new StsClient(
    [
        'profile' => 'oasis-minhao',
        'region'  => 'ap-northeast-1',
    ]
);
$rs       = RedshiftConnection::getConnection(
    [
        "host"     => "oas-dmp-test.cikskyn4dlgm.ap-northeast-1.redshift.amazonaws.com",
        "port"     => 5439,
        "dbname"   => "oasdmp",
        "user"     => "oasdmp",
        "password" => "NU9qEG3nR8",
    ]
);
$localFs  = new ExtendedFilesystem(new ExtendedLocal('/tmp'));
$s3Fs     = new ExtendedFilesystem(new ExtendedAwsS3Adapter(new S3Client($awsConfig), "minhao-dev", "/tmp"));
$importer = new RedshiftImporter(
    $rs,
    $localFs,
    $s3Fs,
    'ap-northeast-1',
    new StsCredentialProvider($sts)
);
$exporter = new RedshiftExporter(
    $rs,
    $localFs,
    $s3Fs,
    'ap-northeast-1',
    new StsCredentialProvider($sts)
);
$columns  = explode(",", "a1,a2,a3,a4,a5,a6,a7");

$dataPath = 'data';
$localFs->put($dataPath, '');
$drd_os = $localFs->appendStream($dataPath);
$writer = new DrdStreamWriter($drd_os, $columns);

for ($i = 0; $i < 10; ++$i) {
    $data = [];
    for ($j = 0; $j < 7; ++$j) {
        $data['a' . ($j + 1)] = mt_rand(1, 10) + $j * 10;
    }
    $writer->writeRecord($data);
}
fclose($drd_os);

//$importer->importFromFile('/out/testing', 'test', $columns, true, true);
//$importer->importFromFile('ddd', 'test', $columns, true, true);
$exporter->exportToFile('/out//testing', 'SELECT * FROM test', false, true, true);

