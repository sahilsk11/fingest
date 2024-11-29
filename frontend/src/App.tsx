import { useState } from "react"
import FileInput from "./components/ui/file_input";

export default function Home() {
  const [files, setFiles] = useState<File[]>([]);

  const handleFileUpload = (file: File) => {
    setFiles(prevFiles => [...prevFiles, file]);
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
