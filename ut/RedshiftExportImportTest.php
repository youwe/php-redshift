<?php
use Oasis\Mlib\AwsWrappers\S3Client;
use Oasis\Mlib\AwsWrappers\StsClient;
use Oasis\Mlib\FlysystemWrappers\ExtendedAwsS3Adapter;
use Oasis\Mlib\FlysystemWrappers\ExtendedFilesystem;
use Oasis\Mlib\FlysystemWrappers\ExtendedLocal;
use Oasis\Mlib\Redshift\DrdStreamReader;
use Oasis\Mlib\Redshift\DrdStreamWriter;
use Oasis\Mlib\Redshift\RedshiftConnection;
use Oasis\Mlib\Redshift\RedshiftExporter;
use Oasis\Mlib\Redshift\RedshiftImporter;
use Oasis\Mlib\Utils\ArrayDataProvider;
use Oasis\Mlib\Utils\DataProviderInterface;
use Symfony\Component\Yaml\Yaml;

/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2016-08-23
 * Time: 20:17
 */
class RedshiftExportImportTest extends PHPUnit_Framework_TestCase
{
    const FIELDS = [
        "a1",
        "a2",
        "a3",
        "a4",
        "a5",
        "a6",
        "a7",
    ];
    
    /** @var  ExtendedFilesystem */
    protected static $localFs;
    /** @var  ExtendedFilesystem */
    protected static $s3Fs;
    protected static $s3Region;
    /** @var  StsClient */
    protected static $sts;
    /** @var  RedshiftConnection */
    protected static $rs;
    
    public static function setUpBeforeClass()
    {
        parent::setUpBeforeClass();
        
        $dataSourceFile = __DIR__ . "/data_source.yml";
        $config         = Yaml::parse(file_get_contents($dataSourceFile));
        
        $dp = new ArrayDataProvider($config);
        
        $awsConfig      = $dp->getMandatory('aws', DataProviderInterface::ARRAY_TYPE);
        self::$s3Region = $dp->getMandatory('aws.region', DataProviderInterface::STRING_TYPE);
        self::$sts      = new StsClient($awsConfig);
        $s3             = new S3Client($awsConfig);
        self::$localFs  = new ExtendedFilesystem(new ExtendedLocal(sys_get_temp_dir()));
        self::$s3Fs     = new ExtendedFilesystem(
            new ExtendedAwsS3Adapter(
                $s3,
                $dp->getMandatory('aws.s3bucket', DataProviderInterface::STRING_TYPE),
                $dp->getMandatory('aws.s3prefix', DataProviderInterface::STRING_TYPE)
            )
        );
        self::$rs       = RedshiftConnection::getConnection(
            $dp->getMandatory('redshift', DataProviderInterface::ARRAY_TYPE)
        );
    }
    
    protected function setUp()
    {
        parent::setUp();
        
        $dropStmt   = <<<SQL
DROP TABLE IF EXISTS "php_redshift_test"
SQL;
        $createStmt = <<<SQL
CREATE TABLE "php_redshift_test" (
    a1 VARCHAR(64),
    a2 VARCHAR(64),
    a3 VARCHAR(64),
    a4 VARCHAR(64),
    a5 VARCHAR(64),
    a6 VARCHAR(64),
    a7 VARCHAR(64)
);

SQL;
        self::$rs->exec($dropStmt);
        self::$rs->exec($createStmt);
    }
    
    public function testDataImport()
    {
        $data = [];
        for ($i = 1; $i <= 5; ++$i) {
            $row = [];
            for ($j = 1; $j <= 7; ++$j) {
                $row['a' . $j] = $j + $i * 10;
            }
            $data[] = $row;
        }
        
        $out    = fopen('php://memory', 'r+');
        $writer = new DrdStreamWriter($out, self::FIELDS);
        foreach ($data as $row) {
            $writer->writeRecord($row);
        }
        rewind($out);
        self::$localFs->putStream('data', $out);
        fclose($out);
        
        $importer = new RedshiftImporter(self::$rs, self::$localFs, self::$s3Fs, self::$s3Region, self::$sts);
        $importer->importFromFile('data', 'php_redshift_test', self::FIELDS, false, true);
        
        $stmt = self::$rs->prepare("SELECT COUNT(*) FROM php_redshift_test");
        $stmt->execute();
        $result = $stmt->fetchColumn();
        
        self::$localFs->delete('data');
        
        self::assertEquals(5, $result);
    }
    
    /**
     * @depends testDataImport
     */
    public function testDataExport()
    {
        $exportPrefix = "redshift_ut_" . time();
        
        $this->testDataImport();
        $exporter = new RedshiftExporter(self::$rs, self::$localFs, self::$s3Fs, self::$s3Region, self::$sts);
        $exporter->exportToFile($exportPrefix, "SELECT * FROM php_redshift_test", false, false, true);
        
        $exportedCount = 0;
        $finder        = self::$localFs->getFinder();
        $finder->path("#^" . preg_quote($exportPrefix, "#") . "#");
        $unloaded = [];
        foreach ($finder as $splFileInfo) {
            $relativePathname = $splFileInfo->getRelativePathname();
            $unloaded[]       = $relativePathname;
            $fh               = self::$localFs->readStream($relativePathname);
            $reader           = new DrdStreamReader($fh, self::FIELDS);
            while ($reader->readRecord()) {
                $exportedCount++;
            }
            fclose($fh);
        }
        foreach ($unloaded as $relativePathname) {
            self::$localFs->delete($relativePathname);
        }
        
        self::assertEquals(5, $exportedCount);
        
    }
}
