<?php

namespace Doctrine\Tests\DBAL\Schema;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Schema\PostgreSqlSchemaManager;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\Tests\DBAL\Mocks;

class PostgreSQLSchemaManagerTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \Doctrine\DBAL\Schema\PostgreSQLSchemaManager
     */
    private $schemaManager;

    /**
     * @var \Doctrine\DBAL\Connection|\PHPUnit_Framework_MockObject_MockObject
     */
    private $connection;

    protected function setUp()
    {
        $driverMock = $this->getMock('Doctrine\DBAL\Driver');
        $platform = $this->getMock('Doctrine\DBAL\Platforms\PostgreSqlPlatform');
        $this->connection = $this->getMock(
            'Doctrine\DBAL\Connection',
            array(),
            array(array('platform' => $platform), $driverMock)
        );
        $this->schemaManager = new PostgreSqlSchemaManager($this->connection, $platform);
    }

    /**
     * @group DBAL-474
     */
    public function testFiltersSequences()
    {
        $configuration = new Configuration();
        $configuration->setFilterSchemaAssetsExpression('/^schema/');

        $sequences = array(
            array('relname' => 'foo', 'schemaname' => 'schema'),
            array('relname' => 'bar', 'schemaname' => 'schema'),
            array('relname' => 'baz', 'schemaname' => ''),
            array('relname' => 'bloo', 'schemaname' => 'bloo_schema'),
        );

        $this->connection->expects($this->any())
            ->method('getConfiguration')
            ->will($this->returnValue($configuration));

        $this->connection->expects($this->at(0))
            ->method('fetchAll')
            ->will($this->returnValue($sequences));

        $this->connection->expects($this->at(1))
            ->method('fetchAll')
            ->will($this->returnValue(array(array('min_value' => 1, 'increment_by' => 1))));

        $this->connection->expects($this->at(2))
            ->method('fetchAll')
            ->will($this->returnValue(array(array('min_value' => 2, 'increment_by' => 2))));

        $this->connection->expects($this->exactly(3))
            ->method('fetchAll');

        $this->assertEquals(
            array(
                new Sequence('schema.foo', 2, 2),
                new Sequence('schema.bar', 1, 1),
            ),
            $this->schemaManager->listSequences('database')
        );
    }

    /**
     * Verify that an existing datetime column in the database
     * that is set to default to the current time will be correctly
     * transformed to "CURRENT_TIMESTAMP" by the schema manager.
     */
    public function testDatetimeDefaultNow()
    {
        // simulate the results of executing the SQL generated
        // by PostgreSqlPlatform::getListTableColumnsSQL()
        $testColumns = array(
            array(
                'field' => 'test_field',
                'type' => 'timestamptz',
                'complete_type' => 'timestamp with time zone',
                'length' => null,
                'default' => 'now()',
                'isnotnull' => false,
                'collation' => null,
                'comment' => null,
                'domain_type' => null,
                'domain_complete_type' => null,
                'pri' => null
            )
        );

        $this->connection->expects($this->at(1))
            ->method('fetchAll')
            ->will($this->returnValue($testColumns));

        $this->schemaManager->getDatabasePlatform()->expects($this->any())
            ->method('getDoctrineTypeMapping')
            ->with($this->equalTo('timestamptz'))
            ->will($this->returnValue('datetimetz'));

        $this->schemaManager->getDatabasePlatform()->expects($this->any())
            ->method('getCurrentTimestampSQL')
            ->will($this->returnValue('CURRENT_TIMESTAMP'));

        $columns = $this->schemaManager->listTableColumns('test_table');

        $this->assertArrayHasKey('test_field', $columns);
        $this->assertEquals('CURRENT_TIMESTAMP', $columns['test_field']->getDefault());
    }

    /**
     * Verify that an existing datetimetz column in the database
     * that is set to default to the current time will be correctly
     * transformed to "CURRENT_TIMESTAMP" by the schema manager.
     */
    public function testDatetimeTzDefaultNow()
    {
        // simulate the results of executing the SQL generated
        // by PostgreSqlPlatform::getListTableColumnsSQL()
        $testColumns = array(
            array(
                'field' => 'test_field',
                'type' => 'timestamp',
                'complete_type' => 'timestamp without time zone',
                'length' => null,
                'default' => 'now()',
                'isnotnull' => false,
                'collation' => null,
                'comment' => null,
                'domain_type' => null,
                'domain_complete_type' => null,
                'pri' => null
            )
        );

        $this->connection->expects($this->at(1))
            ->method('fetchAll')
            ->will($this->returnValue($testColumns));

        $this->schemaManager->getDatabasePlatform()->expects($this->any())
            ->method('getDoctrineTypeMapping')
            ->with($this->equalTo('timestamp'))
            ->will($this->returnValue('datetime'));

        $this->schemaManager->getDatabasePlatform()->expects($this->any())
            ->method('getCurrentTimestampSQL')
            ->will($this->returnValue('CURRENT_TIMESTAMP'));

        $columns = $this->schemaManager->listTableColumns('test_table');

        $this->assertArrayHasKey('test_field', $columns);
        $this->assertEquals('CURRENT_TIMESTAMP', $columns['test_field']->getDefault());
    }

    /**
     * Verify that an existing datetimetz column in the database
     * that is set to default to a timestamp value will be correctly
     * handled by the schema manager.
     */
    public function testDatetimeTzDefaultTimestamp()
    {
        $testTimestamp = '2014-08-29 18:01:01.370568-07';

        // simulate the results of executing the SQL generated
        // by PostgreSqlPlatform::getListTableColumnsSQL()
        $testColumns = array(
            array(
                'field' => 'test_field',
                'type' => 'timestamptz',
                'complete_type' => 'timestamp with time zone',
                'length' => null,
                'default' => "'" . $testTimestamp . "'::timestamp with time zone",
                'isnotnull' => false,
                'collation' => null,
                'comment' => null,
                'domain_type' => null,
                'domain_complete_type' => null,
                'pri' => null
            )
        );

        $this->connection->expects($this->at(1))
            ->method('fetchAll')
            ->will($this->returnValue($testColumns));

        $this->schemaManager->getDatabasePlatform()->expects($this->any())
            ->method('getDoctrineTypeMapping')
            ->with($this->equalTo('timestamptz'))
            ->will($this->returnValue('datetimetz'));

        $this->schemaManager->getDatabasePlatform()->expects($this->any())
            ->method('getCurrentTimestampSQL')
            ->will($this->returnValue('CURRENT_TIMESTAMP'));

        $columns = $this->schemaManager->listTableColumns('test_table');

        $this->assertArrayHasKey('test_field', $columns);
        $this->assertEquals($testTimestamp, $columns['test_field']->getDefault());
    }
}
