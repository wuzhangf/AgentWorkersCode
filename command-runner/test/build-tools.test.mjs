// Optional real toolchain qualification, no model and no simulated test verdicts.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,readFileSync,mkdirSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {CommandRunner} from '../runner.mjs';
async function fixture(fn){const root=mkdtempSync(join(tmpdir(),'command-build-'));const r=new CommandRunner({state:join(root,'state'),token:'non-secret-local-build-test-owner-token'});r.register({contextId:'build',workspace:root,binding:{taskId:'build-fixture'},envAllowlist:['RUSTUP_HOME','CARGO_HOME','JAVA_HOME','DEVELOPER_DIR']});
 let n=0;const run=async(kind,argv)=>{const c=r.create({requestKey:'build-'+(++n),contextId:'build',kind,argv,timeoutMs:180000});for(let i=0;i<9000;i++){const d=r.command(c.commandId);if(['SUCCEEDED','FAILED','UNKNOWN','TIMED_OUT'].includes(d.status))return d;await new Promise(x=>setTimeout(x,20));}throw Error('BUILD_TEST_DEADLINE');};
 try{await fn(root,run);}finally{await r.close();rmSync(root,{recursive:true,force:true});}}
test('real Cargo build and failed-then-passing Rust unit test',{skip:!process.env.NWF_TEST_CARGO},()=>fixture(async(root,run)=>{
 mkdirSync(join(root,'src'));writeFileSync(join(root,'Cargo.toml'),'[package]\nname="runner_fixture"\nversion="0.1.0"\nedition="2024"\n');
 const code=op=>`pub fn sum(a:i32,b:i32)->i32 {a ${op} b}\n#[cfg(test)] mod tests {#[test] fn adds(){assert_eq!(super::sum(2,3),5);}}\n`;
 writeFileSync(join(root,'src/lib.rs'),code('-'));
 assert.equal((await run('build',[process.env.NWF_TEST_CARGO,'build','--offline'])).status,'SUCCEEDED');
 const before=await run('test',[process.env.NWF_TEST_CARGO,'test','--offline']);assert.equal(before.status,'FAILED');assert.match(before.stdout,/1 failed/);
 writeFileSync(join(root,'src/lib.rs'),code('+'));
 const after=await run('test',[process.env.NWF_TEST_CARGO,'test','--offline']);assert.equal(after.status,'SUCCEEDED');assert.match(after.stdout,/1 passed/);
 console.log(JSON.stringify({toolchain:'Cargo/Rust',build:'PASS',before:{exitCode:before.exitCode,failedTests:1},after:{exitCode:after.exitCode,passedTests:1}}));
}));
test('real javac build and Java assertion tests',{skip:!process.env.NWF_TEST_JAVAC||!process.env.NWF_TEST_JAVA},()=>fixture(async(root,run)=>{
 writeFileSync(join(root,'Calculator.java'),'public class Calculator {static int sum(int a,int b){return a+b;}}\n');
 writeFileSync(join(root,'CalculatorTest.java'),'public class CalculatorTest {public static void main(String[] a){if(Calculator.sum(2,3)!=5)throw new AssertionError("sum");if(Calculator.sum(-1,1)!=0)throw new AssertionError("zero");System.out.println("Executed 2 tests, 0 failures");}}\n');
 assert.equal((await run('build',[process.env.NWF_TEST_JAVAC,'Calculator.java','CalculatorTest.java'])).status,'SUCCEEDED');
 const result=await run('test',[process.env.NWF_TEST_JAVA,'-cp','.', 'CalculatorTest']);assert.equal(result.status,'SUCCEEDED');assert.match(result.stdout,/Executed 2 tests, 0 failures/);
 console.log(JSON.stringify({toolchain:'JDK21',build:'PASS',tests:2,failures:0}));
}));
test('real Swift package build and XCTest',{skip:process.env.NWF_TEST_SWIFT!=='1'},()=>fixture(async(root,run)=>{
 mkdirSync(join(root,'Sources','Fixture'),{recursive:true});mkdirSync(join(root,'Tests','FixtureTests'),{recursive:true});
 writeFileSync(join(root,'Package.swift'),'// swift-tools-version: 6.0\nimport PackageDescription\nlet package=Package(name:"Fixture",targets:[.target(name:"Fixture"),.testTarget(name:"FixtureTests",dependencies:["Fixture"])])\n');
 writeFileSync(join(root,'Sources','Fixture','Sum.swift'),'public func sum(_ a:Int,_ b:Int)->Int {a+b}\n');
 writeFileSync(join(root,'Tests','FixtureTests','SumTests.swift'),'import XCTest\n@testable import Fixture\nfinal class SumTests:XCTestCase {func testSum(){XCTAssertEqual(sum(2,3),5)}}\n');
 const built=await run('build',['/usr/bin/swift','build']);assert.equal(built.status,'SUCCEEDED',JSON.stringify({reason:built.reasonCode,proof:built.stopProof,stderr:built.stderr.slice(-2000)}));
 const checked=await run('test',['/usr/bin/swift','test']);assert.equal(checked.status,'SUCCEEDED',JSON.stringify({reason:checked.reasonCode,proof:checked.stopProof,stderr:checked.stderr.slice(-2000)}));assert.match(checked.stdout,/Executed 1 test, with 0 failures/);
 console.log(JSON.stringify({toolchain:'Swift/XCTest',build:'PASS',tests:1,failures:0}));
}));
test('real Maven package and failed-then-passing Surefire test',{skip:!process.env.NWF_TEST_MAVEN},()=>fixture(async(root,run)=>{
 mkdirSync(join(root,'src','main','java'),{recursive:true});mkdirSync(join(root,'src','test','java'),{recursive:true});
 writeFileSync(join(root,'settings.xml'),'<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"/>');
 writeFileSync(join(root,'pom.xml'),'<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>fixture</groupId><artifactId>runner-build</artifactId><version>1.0</version><properties><maven.compiler.source>21</maven.compiler.source><maven.compiler.target>21</maven.compiler.target></properties><dependencies><dependency><groupId>junit</groupId><artifactId>junit</artifactId><version>4.13.2</version><scope>test</scope></dependency></dependencies><build><plugins><plugin><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin><plugin><artifactId>maven-surefire-plugin</artifactId><version>3.5.2</version></plugin></plugins></build></project>');
 const code=op=>`public class Calculator {public static int sum(int a,int b){return a ${op} b;}}\n`;
 writeFileSync(join(root,'src/main/java/Calculator.java'),code('-'));
 writeFileSync(join(root,'src/test/java/CalculatorTest.java'),'import org.junit.Test;import static org.junit.Assert.*;public class CalculatorTest {@Test public void sums(){assertEquals(5,Calculator.sum(2,3));}}\n');
 const args=['-B','-ntp','-s','settings.xml','-gs','settings.xml','-Dmaven.repo.local='+join(root,'m2')];
 const built=await run('build',[process.env.NWF_TEST_MAVEN,...args,'-DskipTests','package']);assert.equal(built.status,'SUCCEEDED',built.stdout.slice(-2500)+built.stderr.slice(-500));
 const before=await run('test',[process.env.NWF_TEST_MAVEN,...args,'test']);assert.equal(before.status,'FAILED');
 assert.match(readFileSync(join(root,'target/surefire-reports/TEST-CalculatorTest.xml'),'utf8'),/failures="1"/);
 writeFileSync(join(root,'src/main/java/Calculator.java'),code('+'));
 const after=await run('test',[process.env.NWF_TEST_MAVEN,...args,'test']);assert.equal(after.status,'SUCCEEDED',after.stdout.slice(-1000));
 const report=readFileSync(join(root,'target/surefire-reports/TEST-CalculatorTest.xml'),'utf8');assert.match(report,/tests="1"/);assert.match(report,/failures="0"/);
 console.log(JSON.stringify({toolchain:'Maven/JUnit',build:'PASS',before:{exitCode:before.exitCode,failedTests:1},after:{exitCode:after.exitCode,passedTests:1},evidence:'Surefire XML'}));
}));
