import { useState } from "react"
import FileInput from "./components/ui/file_input";

export default function Home() {
  return (
    <div>
      <FileUpload />
    </div>
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
