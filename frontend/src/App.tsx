import { useState } from "react"
import FileInput from "./components/ui/file_input";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "./components/ui/accordion";

export default function Home() {
  return (
    <div>
      <FileUpload />
      <Steps />
    </div>
  )
}

function Steps() {
  return (
    <>
      <div className="container mx-auto">
        <Accordion type="single" collapsible>
          <AccordionItem value="item-1">
            <AccordionTrigger>Is it accessible?</AccordionTrigger>
            <AccordionContent>
              Yes. It adheres to the WAI-ARIA design pattern.
            </AccordionContent>
          </AccordionItem>
        </Accordion>
      </div >
    </>
  )
}

function FileUpload() {
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
        console.log(data);
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
