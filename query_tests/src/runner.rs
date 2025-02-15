//! Basic test runner that runs queries in files and compares the output to the expected results

mod parse;
mod setup;

use arrow::record_batch::RecordBatch;
use arrow_util::display::pretty_format_batches;
use query::{
    exec::{Executor, ExecutorType},
    frontend::sql::SqlQueryPlanner,
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    io::LineWriter,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use self::{parse::TestQueries, setup::TestSetup};
use crate::scenarios::{DbScenario, DbSetup};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Can not find case file '{:?}': {}", path, source))]
    NoCaseFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("case file '{:?}' is not UTF8 {}", path, source))]
    CaseFileNotUtf8 {
        path: PathBuf,
        source: std::string::FromUtf8Error,
    },

    #[snafu(display("expected file '{:?}' is not UTF8 {}", path, source))]
    ExpectedFileNotUtf8 {
        path: PathBuf,
        source: std::string::FromUtf8Error,
    },

    #[snafu(display("can not open output file '{:?}': {}", output_path, source))]
    CreatingOutputFile {
        output_path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("can not write to output file '{:?}': {}", output_path, source))]
    WritingToOutputFile {
        output_path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "Contents of output '{:?}' does not match contents of expected '{:?}'",
        output_path,
        expected_path
    ))]
    OutputMismatch {
        output_path: PathBuf,
        expected_path: PathBuf,
    },

    #[snafu(display(
        "Answers produced by scenario {} differ from previous answer\
         \n\nprevious:\n\n{:#?}\ncurrent:\n\n{:#?}\n\n",
        scenario_name,
        previous_results,
        current_results
    ))]
    ScenarioMismatch {
        scenario_name: String,
        previous_results: Vec<String>,
        current_results: Vec<String>,
    },

    #[snafu(display("Test Setup Error: {}", source))]
    SetupError { source: self::setup::Error },

    #[snafu(display("Error writing to runner log: {}", source))]
    LogIO { source: std::io::Error },

    #[snafu(display("IO inner while flushing buffer: {}", source))]
    FlushingBuffer { source: std::io::Error },

    #[snafu(display("Input path has no file stem: '{:?}'", path))]
    NoFileStem { path: PathBuf },

    #[snafu(display("Input path has no parent?!: '{:?}'", path))]
    NoParent { path: PathBuf },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Allow automatic conversion from IO errors
impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::LogIO { source }
    }
}

/// The case runner. It writes its test log output to the `Write`
/// output stream
pub struct Runner<W: Write> {
    log: LineWriter<W>,
}

impl<W: Write> std::fmt::Debug for Runner<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Runner<W>")
    }
}

/// Struct that calls println! to print out its data. Used rather than
/// `std::io::stdout` which is not captured by the result test runner
/// for some reason. This writer expects to get valid utf8 sequences
pub struct PrintlnWriter {}
impl Write for PrintlnWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        println!("{}", String::from_utf8_lossy(buf));
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Runner<PrintlnWriter> {
    /// Create a new runner which writes to std::out
    pub fn new() -> Self {
        let log = LineWriter::new(PrintlnWriter {});
        Self { log }
    }

    pub fn flush(mut self) -> Result<Self> {
        self.log.flush()?;
        Ok(self)
    }
}

impl<W: Write> Runner<W> {
    pub fn new_with_writer(log: W) -> Self {
        let log = LineWriter::new(log);
        Self { log }
    }

    /// Consume self and return the inner `Write` instance
    pub fn into_inner(self) -> Result<W> {
        let Self { mut log } = self;

        log.flush()?;
        let log = match log.into_inner() {
            Ok(log) => log,
            Err(e) => {
                panic!("Error flushing runner's buffer: {}", e.error());
            }
        };

        Ok(log)
    }

    /// Run the test case of the specified `input_path`
    ///
    /// Produces output at `../out/<input_path>.out`
    ///
    /// Compares it to an expected result at `<input_path>.expected`
    pub async fn run(&mut self, input_path: impl Into<PathBuf>) -> Result<()> {
        let input_path = input_path.into();
        // create output and expected output
        let output_path = make_output_path(&input_path)?;
        let expected_path = input_path.with_extension("expected");

        writeln!(self.log, "Running case in {:?}", input_path)?;
        writeln!(self.log, "  writing output to {:?}", output_path)?;
        writeln!(self.log, "  expected output in {:?}", expected_path)?;

        let contents = std::fs::read(&input_path).context(NoCaseFile { path: &input_path })?;
        let contents =
            String::from_utf8(contents).context(CaseFileNotUtf8 { path: &input_path })?;

        writeln!(self.log, "Processing contents:\n{}", contents)?;
        let test_setup = TestSetup::try_from_lines(contents.lines()).context(SetupError)?;
        let queries = TestQueries::from_lines(contents.lines());
        writeln!(self.log, "Using test setup:\n{}", test_setup)?;

        // Make a place to store output files
        let output_file = std::fs::File::create(&output_path).context(CreatingOutputFile {
            output_path: output_path.clone(),
        })?;

        let mut output = vec![];
        output.push(format!("-- Test Setup: {}", test_setup.setup_name()));

        let db_setup = test_setup.get_setup().context(SetupError)?;
        for q in queries.iter() {
            output.push(format!("-- SQL: {}", q));

            output.append(&mut self.run_query(q, db_setup.as_ref()).await?);
        }

        let mut output_file = LineWriter::new(output_file);
        for o in &output {
            writeln!(&mut output_file, "{}", o).with_context(|| WritingToOutputFile {
                output_path: output_path.clone(),
            })?;
        }
        output_file.flush().with_context(|| WritingToOutputFile {
            output_path: output_path.clone(),
        })?;

        std::mem::drop(output_file);

        // Now, compare to expected results
        let expected_data = std::fs::read(&expected_path)
            .ok() // no output is fine
            .unwrap_or_else(Vec::new);

        let expected_contents: Vec<_> = String::from_utf8(expected_data)
            .context(ExpectedFileNotUtf8 {
                path: &expected_path,
            })?
            .lines()
            .map(|s| s.to_string())
            .collect();

        if expected_contents != output {
            let expected_path = make_absolute(&expected_path);
            let output_path = make_absolute(&output_path);

            writeln!(self.log, "Expected output does not match actual output")?;
            writeln!(self.log, "  expected output in {:?}", expected_path)?;
            writeln!(self.log, "  actual output in {:?}", output_path)?;
            writeln!(self.log, "Possibly helpful commands:")?;
            writeln!(self.log, "  # See diff")?;
            writeln!(self.log, "  diff -du {:?} {:?}", expected_path, output_path)?;
            writeln!(self.log, "  # Update expected")?;
            writeln!(self.log, "  cp -f {:?} {:?}", output_path, expected_path)?;
            OutputMismatch {
                output_path,
                expected_path,
            }
            .fail()
        } else {
            Ok(())
        }
    }

    /// runs the specified query against each scenario in `db_setup`
    /// and compares them for equality with each other. If they all
    /// produce the same answer, that answer is returned as pretty
    /// printed strings.
    ///
    /// If there is a mismatch the runner panics
    ///
    /// Note this does not (yet) understand how to compare results
    /// while ignoring output order
    async fn run_query(&mut self, sql: &str, db_setup: &dyn DbSetup) -> Result<Vec<String>> {
        let mut previous_results = vec![];

        for scenario in db_setup.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;

            writeln!(self.log, "Running scenario '{}'", scenario_name)?;
            writeln!(self.log, "SQL: '{:#?}'", sql)?;
            let planner = SqlQueryPlanner::default();
            let num_threads = 1;
            let mut executor = Executor::new(num_threads);

            // hardcode concurrency in tests as by default is is the
            // number of cores, which varies across machines
            executor.config_mut().set_concurrency(4);
            let executor = Arc::new(executor);

            let physical_plan = planner
                .query(db, sql, executor.as_ref())
                .expect("built plan successfully");

            let results: Vec<RecordBatch> = executor
                .collect(physical_plan, ExecutorType::Query)
                .await
                .expect("Running plan");

            let current_results = pretty_format_batches(&results)
                .unwrap()
                .trim()
                .lines()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();

            if !previous_results.is_empty() && previous_results != current_results {
                return ScenarioMismatch {
                    scenario_name,
                    previous_results,
                    current_results,
                }
                .fail();
            }
            previous_results = current_results;
        }
        Ok(previous_results)
    }
}

/// Return output path for input path.
///
/// This converts `some/prefix/in/foo.sql` (or other file extensions) to `some/prefix/out/foo.out`.
fn make_output_path(input: &Path) -> Result<PathBuf> {
    let stem = input.file_stem().context(NoFileStem { path: input })?;

    // go two levels up (from file to dir, from dir to parent dir)
    let parent = input.parent().context(NoParent { path: input })?;
    let parent = parent.parent().context(NoParent { path: parent })?;
    let mut out = parent.to_path_buf();

    // go one level down (from parent dir to out-dir)
    out.push("out");

    // set file name and ext
    // The PathBuf API is somewhat confusing: `set_file_name` will replace the last component (which at this point is
    // the "out"). However we wanna create a file out of the stem and the extension. So as a somewhat hackish
    // workaround first push a placeholder that is then replaced.
    out.push("placeholder");
    out.set_file_name(stem);
    out.set_extension("out");

    Ok(out)
}

/// Return the absolute path to `path`, regardless of if it exists or
/// not on the local filesystem
fn make_absolute(path: &Path) -> PathBuf {
    let mut absolute = std::env::current_dir().expect("can not get current working directory");
    absolute.extend(path);
    absolute
}

#[cfg(test)]
mod test {
    use test_helpers::{assert_contains, tmp_dir};

    use super::*;

    const TEST_INPUT: &str = r#"
-- Runner test, positive
-- IOX_SETUP: TwoMeasurements

-- Only a single query
SELECT * from disk;
"#;

    const EXPECTED_OUTPUT: &str = r#"-- Test Setup: TwoMeasurements
-- SQL: SELECT * from disk;
+-------+--------+--------------------------------+
| bytes | region | time                           |
+-------+--------+--------------------------------+
| 99    | east   | 1970-01-01T00:00:00.000000200Z |
+-------+--------+--------------------------------+
"#;

    #[tokio::test]
    async fn runner_positive() {
        let (_tmp_dir, input_file) = make_in_file(TEST_INPUT);
        let output_path = make_output_path(&input_file).unwrap();
        let expected_path = input_file.with_extension("expected");

        // write expected output
        std::fs::write(&expected_path, EXPECTED_OUTPUT).unwrap();

        let mut runner = Runner::new_with_writer(vec![]);
        let runner_results = runner.run(&input_file).await;

        // ensure that the generated output and expected output match
        let output_contents = read_file(&output_path);
        assert_eq!(output_contents, EXPECTED_OUTPUT);

        // Test should have succeeded
        runner_results.expect("successful run");

        // examine the output log and ensure it contains expected resouts
        let runner_log = runner_to_log(runner);
        assert_contains!(&runner_log, format!("writing output to {:?}", &output_path));
        assert_contains!(
            &runner_log,
            format!("expected output in {:?}", &expected_path)
        );
        assert_contains!(&runner_log, "Setup: TwoMeasurements");
        assert_contains!(
            &runner_log,
            "Running scenario 'Data in open chunk of mutable buffer'"
        );
    }

    #[tokio::test]
    async fn runner_negative() {
        let (_tmp_dir, input_file) = make_in_file(TEST_INPUT);
        let output_path = make_output_path(&input_file).unwrap();
        let expected_path = input_file.with_extension("expected");

        // write incorrect expected output
        std::fs::write(&expected_path, "this is not correct").unwrap();

        let mut runner = Runner::new_with_writer(vec![]);
        let runner_results = runner.run(&input_file).await;

        // ensure that the generated output and expected output match
        let output_contents = read_file(&output_path);
        assert_eq!(output_contents, EXPECTED_OUTPUT);

        // Test should have failed
        let err_string = runner_results.unwrap_err().to_string();
        assert_contains!(
            err_string,
            format!(
                "Contents of output '{:?}' does not match contents of expected '{:?}'",
                &output_path, &expected_path
            )
        );

        // examine the output log and ensure it contains expected resouts
        let runner_log = runner_to_log(runner);
        assert_contains!(&runner_log, format!("writing output to {:?}", &output_path));
        assert_contains!(
            &runner_log,
            format!("expected output in {:?}", &expected_path)
        );
        assert_contains!(&runner_log, "Setup: TwoMeasurements");
    }

    fn make_in_file<C: AsRef<[u8]>>(contents: C) -> (tempfile::TempDir, PathBuf) {
        let dir = tmp_dir().expect("create temp dir");
        let in_dir = dir.path().join("in");
        std::fs::create_dir(&in_dir).expect("create in-dir");

        let out_dir = dir.path().join("out");
        std::fs::create_dir(&out_dir).expect("create out-dir");

        let mut file = in_dir;
        file.push("foo.sql");

        std::fs::write(&file, contents).expect("writing data to temp file");
        (dir, file)
    }

    fn read_file(path: &Path) -> String {
        let output_contents = std::fs::read(path).expect("Can read file");
        String::from_utf8(output_contents).expect("utf8")
    }

    fn runner_to_log(runner: Runner<Vec<u8>>) -> String {
        let runner_log = runner.into_inner().expect("getting inner");
        String::from_utf8(runner_log).expect("output was utf8")
    }
}
