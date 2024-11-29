/**
 * v0 by Vercel.
 * @see https://v0.dev/t/aDFucFbMyb8
 * Documentation: https://v0.dev/docs#integrating-generated-code-into-your-nextjs-app
 */
import { Card, CardContent, CardFooter } from "@/components/ui/card"
// import { Label } from "@/components/ui/label"
// import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"

export default function FileInput({ onFileUpload, files, removeFile }: { onFileUpload: (file: File) => void; files: File[]; removeFile: (file: File) => void; }) {
  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.files) {
      Array.from(event.target.files).forEach(file => {
        onFileUpload(file);
      });
      event.target.value = "";
    }
  }

  return (
    <Card>
      <CardContent className="p-6 space-y-4">
        <input type="file" id="fileInput" hidden onChange={handleFileChange} multiple />
        <label htmlFor="fileInput" className="border-2 border-dashed border-gray-200 rounded-lg flex flex-col gap-1 p-6 items-center cursor-pointer">
          <FileIcon className="w-12 h-12" />
          <span className="text-sm font-medium text-gray-500">Drag and drop files or click to browse</span>
          <span className="text-xs text-gray-500">PDF, image, video, or audio</span>
        </label>

        <div className="mt-4">
          {files.map(file => (
            <DisplayAddedFile key={file.name} keyName={file.name} file={file} removeFile={removeFile} />
          ))}
        </div>
      </CardContent>
      <CardFooter className="flex justify-center">
        <Button size="lg" disabled={files.length === 0}>Upload</Button>
      </CardFooter>
    </Card>
  )
}

function DisplayAddedFile(
  {
    file,
    removeFile,
    keyName,
  }: {
    file: File;
    removeFile: (file: File) => void;
    keyName: string;
  }) {
    function formatFileName(fileName: string) {
        return fileName.length > 25 ? `${fileName.substring(0, 10)}...${fileName.substring(fileName.length - 10)}` : fileName
    }
  return (
    <div key={keyName} className="flex justify-between items-center mt-2">
      <p className="text-gray-600 text-sm">{formatFileName(file.name)} ({(file.size / (1024 * 1024)).toFixed(2)} MB)</p>
      <button onClick={() => removeFile(file)} className="text-grey-400">
        <X />
      </button>
    </div>
  )
}
function FileIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg
      {...props}
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z" />
      <path d="M14 2v4a2 2 0 0 0 2 2h4" />
    </svg>
  )
}

function X(

) {
  return <svg
    xmlns="http://www.w3.org/2000/svg"
    width="20"
    height="20"
    viewBox="0 0 24 24"
    fill="none"
    stroke="grey"
    strokeWidth="1.8"
    strokeLinecap="round"
    strokeLinejoin="round"
    className="lucide lucide-x">
    <path d="M18 6 6 18" />
    <path d="m6 6 12 12" />
  </svg>
}