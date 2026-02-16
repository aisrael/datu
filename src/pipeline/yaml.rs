use crate::Result;
use crate::pipeline::RecordBatchReaderSource;
use crate::pipeline::Step;
use crate::pipeline::WriteYamlArgs;
use crate::pipeline::display::write_record_batches_as_yaml;

/// Pipeline step that writes record batches to a YAML file (sequence of row objects).
pub struct WriteYamlStep {
    pub args: WriteYamlArgs,
}

impl Step for WriteYamlStep {
    type Input = RecordBatchReaderSource;
    type Output = ();

    fn execute(self, mut input: Self::Input) -> Result<Self::Output> {
        let path = self.args.path.as_str();
        let file = std::fs::File::create(path)?;
        let mut reader = input.get()?;
        write_record_batches_as_yaml(&mut *reader, file, self.args.sparse)?;
        Ok(())
    }
}
