import { useEffect, useState } from "react"
import FileInput from "./components/ui/file_input";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "./components/ui/accordion";
import { ImportRunUpdate } from "./api";

export default function Home() {
  const [importRunId, setImportRunId] = useState<string | null>(null);

  return (
    <div className="grid gap-20">
      <FileUpload setImportRunId={setImportRunId} />
      <Steps importRunId={importRunId} />
    </div>
  )
}

function Steps({ importRunId }: { importRunId: string | null }) {
  const [updates, setUpdates] = useState<ImportRunUpdate[]>([]);

  const pollIntervalMs = 300;
  const maxPollingTime = 30000; // 30 seconds

  useEffect(() => {
    if (!importRunId) {
      return;
    }

    const interval = setInterval(() => {
      console.log("polling for updates");
      fetch(`http://localhost:3010/list-import-run-updates`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          importRunId: importRunId,
        }),
      })
        .then((response) => response.json())
        .then((data) => {
          setUpdates(data.updates);
        });
    }, pollIntervalMs);

    setTimeout(() => {
      clearInterval(interval);
    }, maxPollingTime);

    return () => clearInterval(interval);
  }, [importRunId]);

  return (
    <>
      <div className="container mx-auto max-w-3xl">
        {updates.map((update, index) => (
          <Accordion key={index} type="single" collapsible>
            <AccordionItem value="item-1">
              <AccordionTrigger>{update.status.charAt(0).toUpperCase() + update.status.slice(1).replace(/\.$/, '')}</AccordionTrigger>
              <AccordionContent>
                {update.description ? update.description.charAt(0).toUpperCase() + update.description.slice(1) + (update.description.endsWith('.') ? '' : '.') : ''}
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        ))}
      </div>
    </>
  )
}

function FileUpload({ setImportRunId }: { setImportRunId: (importRunId: string) => void }) {
  const [files, setFiles] = useState<File[]>([]);

  function postFile(file: File) {
    const formData = new FormData();
    formData.append("file", file);

    fetch("http://localhost:3010/upload", {
      method: "POST",
      body: formData,
    })
      .then((response) => response.json())
      .then((data) => {
        setImportRunId(data.importRunId);
      });
  }

  const handleFileUpload = (file: File) => {
    setFiles(prevFiles => [...prevFiles, file]);
    postFile(file);
  };

  const removeFile = (fileToRemove: File) => {
    setFiles(prevFiles => prevFiles.filter(file => file !== fileToRemove));
  };

  return (
    <div>
      <div className="flex justify-center items-center mt-10">
        <FileInput onFileUpload={handleFileUpload} files={files} removeFile={removeFile} />
      </div>
    </div>
  )
}
